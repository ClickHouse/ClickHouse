---
toc_priority: 48
toc_title: Access Control and Account Management
---

# Access Control and Account Management {#access-control}

ClickHouse supports access control management based on [RBAC](https://en.wikipedia.org/wiki/Role-based_access_control) approach.

ClickHouse access entities:
- [User account](#user-account-management)
- [Role](#role-management)
- [Row Policy](#row-policy-management)
- [Settings Profile](#settings-profiles-management)
- [Quota](#quotas-management)

You can configure access entities using:

- SQL-driven workflow.

    You need to [enable](#enabling-access-control) this functionality.

- Server [configuration files](configuration-files.md) `users.xml` and `config.xml`.

We recommend using SQL-driven workflow. Both of the configuration methods work simultaneously, so if you use the server configuration files for managing accounts and access rights, you can softly move to SQL-driven workflow. 

!!! note "Warning"
    You can't manage the same access entity by both configuration methods simultaneously.


## Usage {#access-control-usage}

By default, the ClickHouse server provides the user account `default` which is not allowed using SQL-driven access control and account management but have all the rights and permissions. The `default` user account is used in any cases when the username is not defined, for example, at login from client or in distributed queries. In distributed query processing a default user account is used, if the configuration of the server or cluster doesn’t specify the [user and password](../engines/table-engines/special/distributed.md) properties.

If you just start using ClickHouse, you can use the following scenario:

1. [Enable](#enabling-access-control) SQL-driven access control and account management for the `default` user.
2. Login under the `default` user account and create all the required users. Don't forget to create an administrator account (`GRANT ALL ON *.* WITH GRANT OPTION TO admin_user_account`).
3. [Restrict permissions](settings/permissions-for-queries.md#permissions_for_queries) for the `default` user and disable SQL-driven access control and account management for it.

### Properties of Current Solution {#access-control-properties}

- You can grant permissions for databases and tables even if they are not exist.
- If a table was deleted, all the privileges that correspond to this table are not revoked. So, if a new table is created later with the same name all the privileges become again actual. To revoke privileges corresponding to the deleted table, you need to perform, for example, the `REVOKE ALL PRIVILEGES ON db.table FROM ALL` query.
- There is no lifetime settings for privileges.

## User account {#user-account-management}

A user account is an access entity that allows to authorize someone in ClickHouse. A user account contains:

- Identification information.
- [Privileges](../sql-reference/statements/grant.md#grant-privileges) that define a scope of queries the user can perform.
- Hosts from which connection to the ClickHouse server is allowed.
- Granted and default roles.
- Settings with their constraints that apply by default at the user's login.
- Assigned settings profiles.

Privileges to a user account can be granted by the [GRANT](../sql-reference/statements/grant.md) query or by assigning [roles](#role-management). To revoke privileges from a user, ClickHouse provides the [REVOKE](../sql-reference/statements/revoke.md) query. To list privileges for a user, use the - [SHOW GRANTS](../sql-reference/statements/show.md#show-grants-statement) statement.

Management queries:

- [CREATE USER](../sql-reference/statements/create.md#create-user-statement)
- [ALTER USER](../sql-reference/statements/alter.md#alter-user-statement)
- [DROP USER](../sql-reference/statements/misc.md#drop-user-statement)
- [SHOW CREATE USER](../sql-reference/statements/show.md#show-create-user-statement)

### Settings Applying {#access-control-settings-applying}

Settings can be set by different ways: for a user account, in its granted roles and settings profiles. At a user login, if a setting is set in different access entities, the value and constrains of this setting are applied by the following priorities (from higher to lower):

1. User account setting.
2. The settings of default roles of the user account. If a setting is set in some roles, then order of the setting applying is undefined.
3. The settings in settings profiles assigned to a user or to its default roles. If a setting is set in some profiles, then order of setting applying is undefined.
4. Settings applied to all the server by default or from the [default profile](server-configuration-parameters/settings.md#default-profile).


## Role {#role-management}

Role is a container for access entities that can be granted to a user account.

Role contains:

- [Privileges](../sql-reference/statements/grant.md#grant-privileges)
- Settings and constraints
- List of granted roles

Management queries:

- [CREATE ROLE](../sql-reference/statements/create.md#create-role-statement)
- [ALTER ROLE](../sql-reference/statements/alter.md#alter-role-statement)
- [DROP ROLE](../sql-reference/statements/misc.md#drop-role-statement)
- [SET ROLE](../sql-reference/statements/misc.md#set-role-statement)
- [SET DEFAULT ROLE](../sql-reference/statements/misc.md#set-default-role-statement)
- [SHOW CREATE ROLE](../sql-reference/statements/show.md#show-create-role-statement)

Privileges to a role can be granted by the [GRANT](../sql-reference/statements/grant.md) query. To revoke privileges from a role ClickHouse provides the [REVOKE](../sql-reference/statements/revoke.md) query.

## Row Policy {#row-policy-management}

Row policy is a filter that defines which or rows is available for a user or for a role. Row policy contains filters for one specific table and list of roles and/or users which should use this row policy.

Management queries:

- [CREATE ROW POLICY](../sql-reference/statements/create.md#create-row-policy-statement)
- [ALTER ROW POLICY](../sql-reference/statements/alter.md#alter-row-policy-statement)
- [DROP ROW POLICY](../sql-reference/statements/misc.md#drop-row-policy-statement)
- [SHOW CREATE ROW POLICY](../sql-reference/statements/show.md#show-create-row-policy-statement)


## Settings Profile {#settings-profiles-management}

Settings profile is a collection of [settings](settings/index.md). Settings profile contains settings and constraints, and list of roles and/or users to which this quota is applied.

Management queries:

- [CREATE SETTINGS PROFILE](../sql-reference/statements/create.md#create-settings-profile-statement)
- [ALTER SETTINGS PROFILE](../sql-reference/statements/alter.md#alter-settings-profile-statement)
- [DROP SETTINGS PROFILE](../sql-reference/statements/misc.md#drop-settings-profile-statement)
- [SHOW CREATE SETTINGS PROFILE](../sql-reference/statements/show.md#show-create-settings-profile-statement)


## Quota {#quotas-management}

Quota limits resource usage. See [Quotas](quotas.md).

Quota contains a set of limits for some durations, and list of roles and/or users which should use this quota.

Management queries:

- [CREATE QUOTA](../sql-reference/statements/create.md#create-quota-statement)
- [ALTER QUOTA](../sql-reference/statements/alter.md#alter-quota-statement)
- [DROP QUOTA](../sql-reference/statements/misc.md#drop-quota-statement)
- [SHOW CREATE QUOTA](../sql-reference/statements/show.md#show-create-quota-statement)


## Enabling SQL-driven Access Control and Account Management {#enabling-access-control}

- Setup a directory for configurations storage.

    ClickHouse stores access entity configurations in the folder set in the [access_control_path](server-configuration-parameters/settings.md#access_control_path) server configuration parameter.

- Enable SQL-driven access control and account management for at least one user account.

    By default, SQL-driven access control and account management is turned off for all users. You need to configure at least one user in the `users.xml` configuration file and assign 1 to the [access_management](settings/settings-users.md#access_management-user-setting) setting.


[Original article](https://clickhouse.tech/docs/en/operations/access_rights/) <!--hide-->
