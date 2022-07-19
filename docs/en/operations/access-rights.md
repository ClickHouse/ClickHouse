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

-   SQL-driven workflow.

    You need to [enable](#enabling-access-control) this functionality.

-   Server [configuration files](../operations/configuration-files.md) `users.xml` and `config.xml`.

We recommend using SQL-driven workflow. Both of the configuration methods work simultaneously, so if you use the server configuration files for managing accounts and access rights, you can smoothly switch to SQL-driven workflow.

!!! note "Warning"
    You can’t manage the same access entity by both configuration methods simultaneously.

To see all users, roles, profiles, etc. and all their grants use [SHOW ACCESS](../sql-reference/statements/show.md#show-access-statement) statement.

## Usage {#access-control-usage}

By default, the ClickHouse server provides the `default` user account which is not allowed using SQL-driven access control and account management but has all the rights and permissions. The `default` user account is used in any cases when the username is not defined, for example, at login from client or in distributed queries. In distributed query processing a default user account is used, if the configuration of the server or cluster does not specify the [user and password](../engines/table-engines/special/distributed.md) properties.

If you just started using ClickHouse, consider the following scenario:

1.  [Enable](#enabling-access-control) SQL-driven access control and account management for the `default` user.
2.  Log in to the `default` user account and create all the required users. Don’t forget to create an administrator account (`GRANT ALL ON *.* TO admin_user_account WITH GRANT OPTION`).
3.  [Restrict permissions](../operations/settings/permissions-for-queries.md#permissions_for_queries) for the `default` user and disable SQL-driven access control and account management for it.

### Properties of Current Solution {#access-control-properties}

-   You can grant permissions for databases and tables even if they do not exist.
-   If a table was deleted, all the privileges that correspond to this table are not revoked. This means that even if you create a new table with the same name later, all the privileges remain valid. To revoke privileges corresponding to the deleted table, you need to execute, for example, the `REVOKE ALL PRIVILEGES ON db.table FROM ALL` query.
-   There are no lifetime settings for privileges.

## User Account {#user-account-management}

A user account is an access entity that allows to authorize someone in ClickHouse. A user account contains:

-   Identification information.
-   [Privileges](../sql-reference/statements/grant.md#grant-privileges) that define a scope of queries the user can execute.
-   Hosts allowed to connect to the ClickHouse server.
-   Assigned and default roles.
-   Settings with their constraints applied by default at user login.
-   Assigned settings profiles.

Privileges can be granted to a user account by the [GRANT](../sql-reference/statements/grant.md) query or by assigning [roles](#role-management). To revoke privileges from a user, ClickHouse provides the [REVOKE](../sql-reference/statements/revoke.md) query. To list privileges for a user, use the [SHOW GRANTS](../sql-reference/statements/show.md#show-grants-statement) statement.

Management queries:

-   [CREATE USER](../sql-reference/statements/create/user.md)
-   [ALTER USER](../sql-reference/statements/alter/user.md#alter-user-statement)
-   [DROP USER](../sql-reference/statements/drop.md)
-   [SHOW CREATE USER](../sql-reference/statements/show.md#show-create-user-statement)
-   [SHOW USERS](../sql-reference/statements/show.md#show-users-statement)

### Settings Applying {#access-control-settings-applying}

Settings can be configured differently: for a user account, in its granted roles and in settings profiles. At user login, if a setting is configured for different access entities, the value and constraints of this setting are applied as follows (from higher to lower priority):

1.  User account settings.
2.  The settings of default roles of the user account. If a setting is configured in some roles, then order of the setting application is undefined.
3.  The settings from settings profiles assigned to a user or to its default roles. If a setting is configured in some profiles, then order of setting application is undefined.
4.  Settings applied to all the server by default or from the [default profile](../operations/server-configuration-parameters/settings.md#default-profile).

## Role {#role-management}

Role is a container for access entities that can be granted to a user account.

Role contains:

-   [Privileges](../sql-reference/statements/grant.md#grant-privileges)
-   Settings and constraints
-   List of assigned roles

Management queries:

-   [CREATE ROLE](../sql-reference/statements/create/role.md)
-   [ALTER ROLE](../sql-reference/statements/alter/role.md#alter-role-statement)
-   [DROP ROLE](../sql-reference/statements/drop.md)
-   [SET ROLE](../sql-reference/statements/set-role.md)
-   [SET DEFAULT ROLE](../sql-reference/statements/set-role.md#set-default-role-statement)
-   [SHOW CREATE ROLE](../sql-reference/statements/show.md#show-create-role-statement)
-   [SHOW ROLES](../sql-reference/statements/show.md#show-roles-statement)

Privileges can be granted to a role by the [GRANT](../sql-reference/statements/grant.md) query. To revoke privileges from a role ClickHouse provides the [REVOKE](../sql-reference/statements/revoke.md) query.

## Row Policy {#row-policy-management}

Row policy is a filter that defines which of the rows are available to a user or a role. Row policy contains filters for one particular table, as well as a list of roles and/or users which should use this row policy.

!!! note "Warning"
    Row policies makes sense only for users with readonly access. If user can modify table or copy partitions between tables, it defeats the restrictions of row policies.

Management queries:

-   [CREATE ROW POLICY](../sql-reference/statements/create/row-policy.md)
-   [ALTER ROW POLICY](../sql-reference/statements/alter/row-policy.md#alter-row-policy-statement)
-   [DROP ROW POLICY](../sql-reference/statements/drop.md#drop-row-policy-statement)
-   [SHOW CREATE ROW POLICY](../sql-reference/statements/show.md#show-create-row-policy-statement)
-   [SHOW POLICIES](../sql-reference/statements/show.md#show-policies-statement)

## Settings Profile {#settings-profiles-management}

Settings profile is a collection of [settings](../operations/settings/index.md). Settings profile contains settings and constraints, as well as a list of roles and/or users to which this profile is applied.

Management queries:

-   [CREATE SETTINGS PROFILE](../sql-reference/statements/create/settings-profile.md#create-settings-profile-statement)
-   [ALTER SETTINGS PROFILE](../sql-reference/statements/alter/settings-profile.md#alter-settings-profile-statement)
-   [DROP SETTINGS PROFILE](../sql-reference/statements/drop.md#drop-settings-profile-statement)
-   [SHOW CREATE SETTINGS PROFILE](../sql-reference/statements/show.md#show-create-settings-profile-statement)
-   [SHOW PROFILES](../sql-reference/statements/show.md#show-profiles-statement)

## Quota {#quotas-management}

Quota limits resource usage. See [Quotas](../operations/quotas.md).

Quota contains a set of limits for some durations, as well as a list of roles and/or users which should use this quota.

Management queries:

-   [CREATE QUOTA](../sql-reference/statements/create/quota.md)
-   [ALTER QUOTA](../sql-reference/statements/alter/quota.md#alter-quota-statement)
-   [DROP QUOTA](../sql-reference/statements/drop.md#drop-quota-statement)
-   [SHOW CREATE QUOTA](../sql-reference/statements/show.md#show-create-quota-statement)
-   [SHOW QUOTA](../sql-reference/statements/show.md#show-quota-statement)
-   [SHOW QUOTAS](../sql-reference/statements/show.md#show-quotas-statement)

## Enabling SQL-driven Access Control and Account Management {#enabling-access-control}

-   Setup a directory for configurations storage.

    ClickHouse stores access entity configurations in the folder set in the [access_control_path](../operations/server-configuration-parameters/settings.md#access_control_path) server configuration parameter.

-   Enable SQL-driven access control and account management for at least one user account.

    By default, SQL-driven access control and account management is disabled for all users. You need to configure at least one user in the `users.xml` configuration file and set the value of the [access_management](../operations/settings/settings-users.md#access_management-user-setting) setting to 1.

[Original article](https://clickhouse.tech/docs/en/operations/access_rights/) <!--hide-->
