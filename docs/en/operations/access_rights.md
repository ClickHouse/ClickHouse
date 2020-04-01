# Access Control and Account Management {#access-control}

ClickHouse support access control management based on [RBAC](https://en.wikipedia.org/wiki/Role-based_access_control) approach.

Elements of ClickHouse access control model:
- [User account](#user-account-management)
- [Role](#role-management)
- [Row Policy](#row-policy-management)

!!! info "Note"
    Some of the access control functionality is available through the [users.xml](settings/settings_users.md) server configuration files, but we don't recommend using this way of user permissions management. Don't combine these methods of access control management, or it may cause unexpected exceptions.


## User account {#user-account-management}

A user account is a configuration that allows to authorize someone in ClickHouse. A user account contains:

- Identification information.
- [Privileges](../query_language/grant.md#grant-privileges) that define a scope of queries the user can perform.
- [Row policies](#row-policy-management) that define filters for rows available for a user.
- Session settings that apply by default at the user's login.

Privileges to a user account can be granted by the [GRANT](../query_language/grant.md) query or by assigning [roles](#role-management). To revoke privileges from a user ClickHouse provides the [REVOKE](../query_language/revoke.md) query. To list privileges for a user, use the - [SHOW GRANTS](../query_language/show.md#show-grants-statement) statement.

To assign a row policy to a role, use the [CREATE ROW POLICY](../query_language/create.md#create-row-policy-statement) or the [ALTER ROW POLICY](../query_language/alter.md#alter-row-policy-statement) query.



User management queries:

- [CREATE USER](../query_language/create.md#create-user-statement)
- [ALTER USER](../query_language/alter.md#alter-user-statement)
- [DROP USER](../query_language/misc.md#drop-user-statement)
- [SHOW CREATE USER](../query_language/show.md#show-create-user-statement)


## Role {#role-management}

Role is a set of [privileges](../query_language/grant.md#grant-privileges) and [row policies](#row-policy-management). A user assigned with a role gets all the privileges of this role and constraints of row policies.

Role management queries:

- [CREATE ROLE](../query_language/create.md#create-role-statement)
- [ALTER ROLE](../query_language/alter.md#alter-role-statement)
- [DROP ROLE](../query_language/misc.md#drop-role-statement)
- [SET ROLE](../query_language/misc.md#set-role-statement)
- [SET DEFAULT ROLE](../query_language/misc.md#set-default-role-statement)
- [SHOW CREATE ROLE](../query_language/show.md#show-create-role-statement)

Privileges to a role can be granted by the [GRANT](../query_language/grant.md) query. To revoke privileges from a role ClickHouse provides the [REVOKE](../query_language/revoke.md) query. To assign a row policy to a role, use the [CREATE ROW POLICY](../query_language/create.md#create-row-policy-statement) or the [ALTER ROW POLICY](../query_language/alter.md#alter-row-policy-statement) query.


## Row Policy {#row-policy-management}

Row policy is a filter that defines which or rows is available for a user or for a role.

Row management queries:

- [CREATE ROW POLICY](../query_language/create.md#create-row-policy-statement)
- [ALTER ROW POLICY](../query_language/alter.md#alter-row-policy-statement)
- [DROP ROW POLICY](../query_language/misc.md#drop-row-policy-statement)
- [SHOW CREATE ROW POLICY](../query_language/show.md#show-create-row-policy-statement)


[Original article](https://clickhouse.tech/docs/en/operations/access_rights/) <!--hide-->
