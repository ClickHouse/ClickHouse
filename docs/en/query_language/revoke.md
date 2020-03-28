# REVOKE

Revokes privileges from users or roles.

## Syntax {#revoke-syntax}

**Revoking privileges from users**

``` sql
REVOKE privilege[(column_name [,...])] [,...] ON {db.table|db.*|*.*|table|*} FROM {user | CURRENT_USER} [,...] | ALL | ALL EXCEPT {user | CURRENT_USER} [,...]
```

**Revoking roles from users**

``` sql
REVOKE [ADMIN OPTION FOR] role [,...] FROM {user | role | CURRENT_USER} [,...] | ALL | ALL EXCEPT {user_name | role_name | CURRENT_USER} [,...]
```

## Description {#revoke-description}

To revoke some privilege you can use a privilege of wider scope then you plan to revoke. For example, if a user has the `SELECT (x,y)` privilege, administrator can perform `REVOKE SELECT(x,y) ...`, or `REVOKE SELECT * ...`, or even `REVOKE ALL PRIVILEGES ...` query to revoke this privilege.


### Partial Revokes {partial-revokes-dscr}

By default you can't revoke a part of a privilege. For example, if a user has the `SELECT *.*` privilege you can't revoke from it a privilege to read date from some table or database.

You can enable to revoke a privilege for subobject of the initial object by the [partial_revokes](../operations/settings/settings.md#partial-revokes-setting) setting. If `partial_revokes = 1` you can, for example? revoke a privilege to read from some individual database or a table.


