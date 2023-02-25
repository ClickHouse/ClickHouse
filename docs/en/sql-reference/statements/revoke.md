---
slug: /en/sql-reference/statements/revoke
sidebar_position: 39
sidebar_label: REVOKE
---

# REVOKE Statement

Revokes privileges from users or roles.

## Syntax

**Revoking privileges from users**

``` sql
REVOKE [ON CLUSTER cluster_name] privilege[(column_name [,...])] [,...] ON {db.table|db.*|*.*|table|*} FROM {user [AS USER] | role [AS ROLE] | user_and_role AS BOTH | CURRENT_USER} [,...] | ALL | ALL EXCEPT {user [AS USER] | role [AS ROLE] | user_and_role AS BOTH | CURRENT_USER} [,...]
```

**Revoking roles from users**

``` sql
REVOKE [ON CLUSTER cluster_name] [ADMIN OPTION FOR] role [,...] FROM {user [AS USER] | role [AS ROLE] | user_and_role AS BOTH | CURRENT_USER} [,...] | ALL | ALL EXCEPT {user_name [AS USER] | role_name [AS ROLE] | CURRENT_USER} [,...]
```

## Description

To revoke some privilege you can use a privilege of a wider scope than you plan to revoke. For example, if a user has the `SELECT (x,y)` privilege, administrator can execute `REVOKE SELECT(x,y) ...`, or `REVOKE SELECT * ...`, or even `REVOKE ALL PRIVILEGES ...` query to revoke this privilege.
If `AS {USER|ROLE|BOTH}` is not used and a user and a role with the same name exist, only user's permissions will be revoked.

### Partial Revokes

You can revoke a part of a privilege. For example, if a user has the `SELECT *.*` privilege you can revoke from it a privilege to read data from some table or a database.

## Examples

Grant the `john` user account with a privilege to select from all the databases, excepting the `accounts` one:

``` sql
GRANT SELECT ON *.* TO john;
REVOKE SELECT ON accounts.* FROM john;
```

Grant the `mira` user account with a privilege to select from all the columns of the `accounts.staff` table, excepting the `wage` one.

``` sql
GRANT SELECT ON accounts.staff TO mira;
REVOKE SELECT(wage) ON accounts.staff FROM mira;
```

[Original article](https://clickhouse.com/docs/en/operations/settings/settings/)
