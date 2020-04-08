---
toc_priority: 40
toc_title: REVOKE
---

# REVOKE

Revokes privileges from users or roles.

## Syntax {#revoke-syntax}

**Revoking privileges from users**

``` sql
REVOKE [ON CLUSTER] privilege[(column_name [,...])] [,...] ON {db.table|db.*|*.*|table|*} FROM {user | CURRENT_USER} [,...] | ALL | ALL EXCEPT {user | CURRENT_USER} [,...]
```

**Revoking roles from users**

``` sql
REVOKE [ON CLUSTER] [ADMIN OPTION FOR] role [,...] FROM {user | role | CURRENT_USER} [,...] | ALL | ALL EXCEPT {user_name | role_name | CURRENT_USER} [,...]
```

## Description {#revoke-description}

To revoke some privilege you can use a privilege of wider scope then you plan to revoke. For example, if a user has the `SELECT (x,y)` privilege, administrator can perform `REVOKE SELECT(x,y) ...`, or `REVOKE SELECT * ...`, or even `REVOKE ALL PRIVILEGES ...` query to revoke this privilege.


### Partial Revokes {partial-revokes-dscr}

You can revoke a part of a privilege. For example, if a user has the `SELECT *.*` privilege you can revoke from it a privilege to read data from some table or a database.

[Original article](https://clickhouse.tech/docs/en/operations/settings/settings/) <!-- hide -->
