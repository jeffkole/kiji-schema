/**
 * (c) Copyright 2013 WibiData, Inc.
 *
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.kiji.schema.security;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.security.access.AccessControlLists;
import org.apache.hadoop.hbase.security.access.AccessControllerProtocol;
import org.apache.hadoop.hbase.security.access.Permission.Action;
import org.apache.hadoop.hbase.security.access.UserPermission;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.annotations.ApiAudience;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiSystemTable;
import org.kiji.schema.KijiURI;
import org.kiji.schema.hbase.KijiManagedHBaseTableName;
import org.kiji.schema.impl.HBaseKiji;
import org.kiji.schema.impl.HTableInterfaceFactory;
import org.kiji.schema.impl.Versions;

/**
 * The default implementation of KijiSecurityManager.
 *
 * <p>KijiSecurityManager manages access control for a Kiji instance.</p>
 *
 * <p>The current version of Kiji security (security-0.1) is instance-level only.  Users can have
 * READ, WRITE, and/or GRANT access on a Kiji instance.</p>
 */
@ApiAudience.Private
final class KijiSecurityManagerImpl implements KijiSecurityManager {
  private static final Logger LOG = LoggerFactory.getLogger(KijiSecurityManagerImpl.class);

  /** The Kiji instance this manages. */
  private final KijiURI mInstanceUri;

  /** A handle to the Kiji this manages. */
  private final Kiji mKiji;

  /** The system table for the instance this manages. */
  private final KijiSystemTable mSystemTable;

  /** The HBase ACL (Access Control List) table to use. */
  private final AccessControllerProtocol mAccessController;

  /**
   * Constructs a new KijiSecurityManager for an instance, with the specified configuration.
   *
   * <p>A KijiSecurityManager cannot be constructed for an instance if the instance does not have a
   * security version greater than or equal to security-0.1 (that is, if security is not enabled).
   * </p>
   *
   * @param instanceUri is the URI of the instance this KijiSecurityManager will manage.
   * @param conf is the Hadoop configuration to use.
   * @param tableFactory is the table factory to use to access the HBase ACL table.
   * @throws IOException on I/O error.
   * @throws KijiSecurityException if the Kiji security version is not compatible with
   *     KijiSecurityManager.
   */
  KijiSecurityManagerImpl(
      KijiURI instanceUri,
      Configuration conf,
      HTableInterfaceFactory tableFactory) throws IOException {
    mInstanceUri = instanceUri;

    // Get the access controller.
    HTableInterface accessControlTable = tableFactory
        .create(conf, new String(AccessControlLists.ACL_TABLE_NAME, Charsets.UTF_8));
    mAccessController = accessControlTable.coprocessorProxy(
        AccessControllerProtocol.class,
        HConstants.EMPTY_START_ROW);

    mKiji = Kiji.Factory.get().open(mInstanceUri);
    mSystemTable = mKiji.getSystemTable();

    // If the Kiji has security version lower than MIN_SECURITY_VERSION, then KijiSecurityManager
    // can't be instantiated.
    if (mSystemTable.getSecurityVersion().compareTo(Versions.MIN_SECURITY_VERSION) < 0) {
      throw new KijiSecurityException("Cannot create a KijiSecurityManager for security version "
          + mSystemTable.getSecurityVersion() + ". Version must be "
          + Versions.MIN_SECURITY_VERSION + " or higher.");
    }
  }

  /** {@inheritDoc} */
  @Override
  public void grant(KijiUser user, KijiPermissions.Action action)
      throws IOException {
    KijiPermissions currentPermissions = getPermissions(user);
    KijiPermissions newPermissions = currentPermissions.addAction(action);
    changeInstancePermissions(user, newPermissions);
  }

  /** {@inheritDoc} */
  @Override
  public void grantAll(KijiUser user) throws IOException {
    for (KijiPermissions.Action action : KijiPermissions.Action.values()) {
      grant(user, action);
    }
  }

  /** {@inheritDoc} */
  @Override
  public void revoke(KijiUser user, KijiPermissions.Action action)
      throws IOException {
    KijiPermissions currentPermissions = getPermissions(user);
    KijiPermissions newPermissions = currentPermissions.removeAction(action);
    changeInstancePermissions(user, newPermissions);
  }

  /** {@inheritDoc} */
  @Override
  public void revokeAll(KijiUser user) throws IOException {
    changeInstancePermissions(user, KijiPermissions.emptyPermissions());
  }

  /** {@inheritDoc} */
  @Override
  public void reapplyInstancePermissions() throws IOException {
    Set<KijiUser> allUsers = listUsers();
    for (KijiUser user : allUsers) {
      KijiPermissions permissions = getPermissions(user);
      for (KijiPermissions.Action action : permissions.getActions()) {
        grant(user, action);
      }
    }
  }
  /** {@inheritDoc} */
  @Override
  public void applyPermissionsToNewTable(KijiURI tableURI) throws IOException {
    // The argument must be for a table in the instance this manages.
    Preconditions.checkArgument(
        KijiURI.newBuilder(mInstanceUri).withTableName(tableURI.getTable()).build() == tableURI);
    for (KijiUser user : listUsers()) {
      changeHTablePermissions(user.getNameBytes(),
          KijiManagedHBaseTableName
              .getKijiTableName(tableURI.getInstance(), tableURI.getTable()).toBytes(),
          getPermissions(user).toHBaseActions());
    }
  }

  /** {@inheritDoc} */
  @Override
  public void grantInstanceCreator(KijiUser user) throws IOException {
    Set<KijiUser> currentGrantors = getUsersWithPermission(KijiPermissions.Action.GRANT);
    // This can only be called if there are no grantors, right when the instance is created.
    if (currentGrantors.size() != 0) {
      throw new KijiAccessException(
          "Cannot add user " + user.toString()
          + " to grantors as the instance creator for instance " + mInstanceUri.getInstance()
          + " because there are already grantors for this instance.");
    }
    Set<KijiUser> newGrantor = Collections.singleton(user);
    putUsersWithPermission(KijiPermissions.Action.GRANT, newGrantor);
    grantAll(user);
  }

  /** {@inheritDoc} */
  @Override
  public KijiPermissions getPermissions(KijiUser user) throws IOException {
    KijiPermissions result = KijiPermissions.emptyPermissions();

    for (KijiPermissions.Action action : KijiPermissions.Action.values()) {
      Set<KijiUser> usersWithAction = getUsersWithPermission(action);
      if (usersWithAction.contains(user)) {
        result = result.addAction(action);
      }
    }

    return result;
  }

  /** {@inheritDoc} */
  @Override
  public Set<KijiUser> listUsers() throws IOException {
    Set<KijiUser> allUsers = new HashSet<KijiUser>();
    for (KijiPermissions.Action action : KijiPermissions.Action.values()) {
      allUsers.addAll(getUsersWithPermission(action));
    }

    return allUsers;
  }

  /** {@inheritDoc} */
  @Override
  public void checkCurrentGrantAccess() throws IOException {
    KijiUser currentUser = KijiUser.getCurrentUser();
    if (!getPermissions(currentUser).allowsAction(KijiPermissions.Action.GRANT)) {
      throw new KijiAccessException("User " + currentUser.getName()
          + " does not have GRANT access for instance " + mInstanceUri.toString() + ".");
    }
  }

  /**
   * Updates the permissions in the Kiji system table for a user on this Kiji instance.
   *
   * <p>Use {@link #changeInstancePermissions(KijiUser, KijiPermissions)} instead for updating the
   * permissions in HBase as well as in the Kiji system table.</p>
   *
   * @param user whose permissions to update.
   * @param permissions to be applied to this user.
   * @throws IOException If there is an I/O error.
   */
  private void updatePermissions(KijiUser user, KijiPermissions permissions)
      throws IOException {
    checkCurrentGrantAccess();
    for (KijiPermissions.Action action : KijiPermissions.Action.values()) {
      Set<KijiUser> permittedUsers = getUsersWithPermission(action);
      if (permissions.allowsAction(action)) {
        permittedUsers.add(user);
      } else {
        permittedUsers.remove(user);
      }
      putUsersWithPermission(action, permittedUsers);
    }
  }

  /**
   * Gets a set of users from the key 'key'.
   *
   * @param action specifying the permission to get the users of.
   * @return the list of user with that permission.
   * @throws IOException on I/O exception.
   */
  private Set<KijiUser> getUsersWithPermission(KijiPermissions.Action action) throws IOException {
    byte[] serialized = mSystemTable.getValue(action.getStringKey());
    if (null == serialized) {
      // If the key doesn't exist, no users have been put with that permission yet.
      return new HashSet<KijiUser>();
    } else {
      return KijiUser.deserializeKijiUsers(serialized);
    }
  }

  /**
   * Puts a set of users into the key for action.
   *
   * @param action to put the set of users into.
   * @param users to put to that permission.
   * @throws IOException on I/O exception.
   */
  private void putUsersWithPermission(
      KijiPermissions.Action action,
      Set<KijiUser> users)
      throws IOException {
    mSystemTable.putValue(action.getStringKey(), KijiUser.serializeKijiUsers(users));
  }

  /**
   * Changes the permissions of an instance, by changing the permissions of all the Kiji meta
   * tables, and updating the permissions in the system table.
   *
   * @param user is the User for whom the permissions are being changed.
   * @param permissions is the new permissions for the user.
   * @throws IOException on I/O error.
   */
  private void changeInstancePermissions(
      KijiUser user,
      KijiPermissions permissions) throws IOException {
    // TODO(SCHEMA-533): Lock the Kiji instance when changing permissions.
    LOG.info("Changing user permissions for user " + user
        + " on instance " + mInstanceUri.getInstance()
        + " to actions " + permissions.getActions().toString());

    // Record the changes in the system table.
    updatePermissions(user, permissions);


    // Change permissions of Kiji system tables in HBase.
    KijiPermissions systemTablePermissions;
    // If this is GRANT permission, also add WRITE access to the permissions in the system table.
    if (permissions.allowsAction(KijiPermissions.Action.GRANT)) {
      systemTablePermissions =
          permissions.addAction(KijiPermissions.Action.WRITE);
    } else {
      systemTablePermissions = permissions;
    }
    changeHTablePermissions(user.getNameBytes(),
        KijiManagedHBaseTableName.getSystemTableName(mInstanceUri.getInstance()).toBytes(),
        systemTablePermissions.toHBaseActions());

    // Change permissions of the other Kiji meta tables.
    changeHTablePermissions(user.getNameBytes(),
        KijiManagedHBaseTableName.getMetaTableName(mInstanceUri.getInstance()).toBytes(),
        permissions.toHBaseActions());
    changeHTablePermissions(user.getNameBytes(),
        KijiManagedHBaseTableName.getSchemaHashTableName(mInstanceUri.getInstance()).toBytes(),
        permissions.toHBaseActions());
    changeHTablePermissions(user.getNameBytes(),
        KijiManagedHBaseTableName.getSchemaIdTableName(mInstanceUri.getInstance()).toBytes(),
        permissions.toHBaseActions());

    // Change permissions of all Kiji tables in this instance in HBase.
    Kiji kiji = Kiji.Factory.open(mInstanceUri);
    try {
      for (String kijiTableName : Kiji.Factory.open(mInstanceUri).getTableNames()) {
        byte[] kijiHTableNameBytes =
            KijiManagedHBaseTableName.getKijiTableName(
                mInstanceUri.getInstance(),
                kijiTableName
            ).toBytes();
        changeHTablePermissions(user.getNameBytes(),
            kijiHTableNameBytes,
            permissions.toHBaseActions());
      }
    } finally {
      kiji.release();
    }
  }

  /**
   * Changes the permissions on an HBase table.
   *
   * @param hUser HBase byte representation of the user whose permissions to change.
   * @param hTableName the HBase table to change permissions on.
   * @param hActions for the user on the table.
   * @throws IOException on I/O error, for example if security is not enabled.
   */
  private void changeHTablePermissions(
      byte[] hUser,
      byte[] hTableName,
      Action[] hActions) throws IOException {
    // Get the HBaseAdmin.
    HBaseKiji hBaseKiji = (HBaseKiji) Kiji.Factory.open(mInstanceUri);
    HBaseAdmin mAdmin = hBaseKiji.getHBaseAdmin();
    hBaseKiji.release();

    // Construct the HBase UserPermission to grant.
    UserPermission hTablePermission = new UserPermission(
        hUser,
        hTableName,
        null,
        hActions);

    // Grant the permissions.
    LOG.info("Changing user permissions for user " + new String(hUser, Charsets.UTF_8)
        + " on table " + new String(hTableName, Charsets.UTF_8)
        + " to HBase Actions " + Arrays.toString(hActions));
    LOG.info("Disabling table " + new String(hTableName, Charsets.UTF_8));
    mAdmin.disableTable(hTableName);
    // Grant the permissions.
    mAccessController.grant(hTablePermission);
    LOG.info("Enabling table " + new String(hTableName, Charsets.UTF_8));
    mAdmin.enableTable(hTableName);
    mAdmin.close();
  }

  /** {@inheritDoc} */
  @Override
  public void close() throws IOException {
    mKiji.release();
  }
}
