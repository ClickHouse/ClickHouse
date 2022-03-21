---
toc_priority: 39
toc_title: REVOKE
---

# REVOKE Statement {#revoke}

Revokes privileges from users or roles.

## Syntax {#revoke-syntax}

**Revoking privileges from users**

``` sql
REVOKE [ON CLUSTER cluster_name] privilege[(column_name [,...])] [,...] ON {db.table|db.*|*.*|table|*} FROM {user | CURRENT_USER} [,...] | ALL | ALL EXCEPT {user | CURRENT_USER} [,...]
```

**Revoking roles from users**

``` sql
REVOKE [ON CLUSTER cluster_name] [ADMIN OPTION FOR] role [,...] FROM {user | role | CURRENT_USER} [,...] | ALL | ALL EXCEPT {user_name | role_name | CURRENT_USER} [,...]
```

## Description {#revoke-description}

To revoke some privilege you can use a privilege of a wider scope than you plan to revoke. For example, if a user has the `SELECT (x,y)` privilege, administrator can execute `REVOKE SELECT(x,y) ...`, or `REVOKE SELECT * ...`, or even `REVOKE ALL PRIVILEGES ...` query to revoke this privilege.

### Partial Revokes {#partial-revokes-dscr}

You can revoke a part of a privilege. For example, if a user has the `SELECT *.*` privilege you can revoke from it a privilege to read data from some table or a database.

## Examples {#revoke-example}

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

{## [Original article](https://clickhouse.com/docs/en/operations/settings/settings/) ##}
