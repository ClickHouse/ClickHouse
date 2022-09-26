# grants

Privileges granted to ClickHouse user accounts.

Columns:
-    `user_name` ([Nullable](../../sql-reference/data-types/nullable.md)([String](../../sql-reference/data-types/string.md))) — User name.

-    `role_name` ([Nullable](../../sql-reference/data-types/nullable.md)([String](../../sql-reference/data-types/string.md))) — Role assigned to user account.

-    `access_type` ([Enum8](../../sql-reference/data-types/enum.md)) — Access parameters for ClickHouse user account.

-    `database` ([Nullable](../../sql-reference/data-types/nullable.md)([String](../../sql-reference/data-types/string.md))) — Name of a database.

-    `table` ([Nullable](../../sql-reference/data-types/nullable.md)([String](../../sql-reference/data-types/string.md))) — Name of a table.

-    `column` ([Nullable](../../sql-reference/data-types/nullable.md)([String](../../sql-reference/data-types/string.md))) — Name of a column to which access is granted.

-    `is_partial_revoke` ([UInt8](../../sql-reference/data-types/int-uint.md#uint-ranges)) — Logical value. It shows whether some privileges have been revoked. Possible values:
- `0` — The row describes a partial revoke.
- `1` — The row describes a grant.

-    `grant_option` ([UInt8](../../sql-reference/data-types/int-uint.md#uint-ranges)) — Permission is granted `WITH GRANT OPTION`, see [GRANT](../../sql-reference/statements/grant.md#grant-privigele-syntax).

[Original article](https://clickhouse.com/docs/en/operations/system-tables/grants) <!--hide-->
