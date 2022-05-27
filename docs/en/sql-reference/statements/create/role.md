---
sidebar_position: 40
sidebar_label: ROLE
---

# CREATE ROLE {#create-role-statement}

Creates new [roles](../../../operations/access-rights.md#role-management). Role is a set of [privileges](../../../sql-reference/statements/grant.md#grant-privileges). A [user](../../../sql-reference/statements/create/user.md) assigned a role gets all the privileges of this role.

Syntax:

``` sql
CREATE ROLE [IF NOT EXISTS | OR REPLACE] name1 [, name2 ...]
    [SETTINGS variable [= value] [MIN [=] min_value] [MAX [=] max_value] [READONLY|WRITABLE] | PROFILE 'profile_name'] [,...]
```

## Managing Roles {#managing-roles}

A user can be assigned multiple roles. Users can apply their assigned roles in arbitrary combinations by the [SET ROLE](../../../sql-reference/statements/set-role.md) statement. The final scope of privileges is a combined set of all the privileges of all the applied roles. If a user has privileges granted directly to it’s user account, they are also combined with the privileges granted by roles.

User can have default roles which apply at user login. To set default roles, use the [SET DEFAULT ROLE](../../../sql-reference/statements/set-role.md#set-default-role-statement) statement or the [ALTER USER](../../../sql-reference/statements/alter/user.md#alter-user-statement) statement.

To revoke a role, use the [REVOKE](../../../sql-reference/statements/revoke.md) statement.

To delete role, use the [DROP ROLE](../../../sql-reference/statements/drop#drop-role-statement) statement. The deleted role is being automatically revoked from all the users and roles to which it was assigned.

## Examples {#create-role-examples}

``` sql
CREATE ROLE accountant;
GRANT SELECT ON db.* TO accountant;
```

This sequence of queries creates the role `accountant` that has the privilege of reading data from the `db` database.

Assigning the role to the user `mira`:

``` sql
GRANT accountant TO mira;
```

After the role is assigned, the user can apply it and execute the allowed queries. For example:

``` sql
SET ROLE accountant;
SELECT * FROM db.*;
```
