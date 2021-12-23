# system.grants {#system_tables-grants}

授予ClickHouse用户帐户的权限.

列信息:
-    `user_name` ([Nullable](../../sql-reference/data-types/nullable.md)([String](../../sql-reference/data-types/string.md))) — 用户名称.

-    `role_name` ([Nullable](../../sql-reference/data-types/nullable.md)([String](../../sql-reference/data-types/string.md))) — 分配给用户帐号的角色.

-    `access_type` ([Enum8](../../sql-reference/data-types/enum.md)) — ClickHouse用户帐号的接入参数.

-    `database` ([Nullable](../../sql-reference/data-types/nullable.md)([String](../../sql-reference/data-types/string.md))) — 数据库名称.

-    `table` ([Nullable](../../sql-reference/data-types/nullable.md)([String](../../sql-reference/data-types/string.md))) — 表名称.

-    `column` ([Nullable](../../sql-reference/data-types/nullable.md)([String](../../sql-reference/data-types/string.md))) — 被授予访问权限的列的名称.

-    `is_partial_revoke` ([UInt8](../../sql-reference/data-types/int-uint.md#uint-ranges)) — 逻辑值. 它显示了某些特权是否被取消. 可能的值:
- `0` — 该行描述了部分撤销.
- `1` — 这一行描述了一个授权.

-    `grant_option` ([UInt8](../../sql-reference/data-types/int-uint.md#uint-ranges)) — 被授予`WITH GRANT OPTION` 权限, 参见 [GRANT](../../sql-reference/statements/grant.md#grant-privigele-syntax).

[原始文章](https://clickhouse.com/docs/en/operations/system-tables/grants) <!--hide-->
