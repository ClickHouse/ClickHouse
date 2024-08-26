---
slug: /en/sql-reference/statements/check-grant
sidebar_position: 56
sidebar_label: CHECK GRANT
title: "CHECK GRANT Statement"
---

The `CHECK GRANT` query is used to check whether the current user/role has been granted a specific privilege, and whether the corresponding table/column exists in the memory.

## Syntax

The basic syntax of the query is as follows:

```sql
CHECK GRANT privilege[(column_name [,...])] [,...] ON {db.table|db.*|*.*|table|*}
```

- `privilege` — Type of privilege.

## Examples

If the user used to be granted the privilege, or the role (which is granted with the privilege), and the db.table(column) exists on this node, the response`check_grant` will be `1`. Otherwise, the response `check_grant` will be `0`.

If `table_1.col1` exists and current user is granted by privilege `SELECT`/`SELECT(con)` or role(with privilege), the response is `1`.
```sql
CHECK GRANT SELECT(col1) ON table_1;
```

```text
┌─CHECK_GRANT─┐
│           1 │
└─────────────┘
```
If `table_2.col2` doesn't exists, or current user is not granted by privilege `SELECT`/`SELECT(con)` or role(with privilege), the response is `0`.
```sql
CHECK GRANT SELECT(col2) ON table_2;
```

```text
┌─CHECK_GRANT─┐
│           0 │
└─────────────┘
```
