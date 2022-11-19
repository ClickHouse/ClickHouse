# system.row_policies {#system_tables-row_policies}

包含一个特定表的过滤器, 以及应该使用此行策略的角色和/或用户列表.

列信息:
-    `name` ([String](../../sql-reference/data-types/string.md)) — 行策略的名称.

-    `short_name` ([String](../../sql-reference/data-types/string.md)) — 行策略的短名称. 行策略的名称是复合的，例如：myfilter ON mydb.mytable. 这里 "myfilter ON mydb.mytable" 是行策略的名称, "myfilter" 是它的简称.

-    `database` ([String](../../sql-reference/data-types/string.md)) — 数据库名称.

-    `table` ([String](../../sql-reference/data-types/string.md)) — 表名称.

-    `id` ([UUID](../../sql-reference/data-types/uuid.md)) — 行策略 ID.

-    `storage` ([String](../../sql-reference/data-types/string.md)) — 存储行策略的目录名.

-    `select_filter` ([Nullable](../../sql-reference/data-types/nullable.md)([String](../../sql-reference/data-types/string.md))) — 用于过滤行的条件.

-    `is_restrictive` ([UInt8](../../sql-reference/data-types/int-uint.md#uint-ranges)) — 显示行策略是否限制对行的访问, 参考 [CREATE ROW POLICY](../../sql-reference/statements/create/row-policy.md#create-row-policy-as). 值:
- `0` — 行策略使用 `AS PERMISSIVE` 子句定义.
- `1` — 行策略使用  `AS RESTRICTIVE` 子句定义.

-    `apply_to_all` ([UInt8](../../sql-reference/data-types/int-uint.md#uint-ranges)) — 显示为所有角色和/或用户设置的行策略.

-    `apply_to_list` ([Array](../../sql-reference/data-types/array.md)([String](../../sql-reference/data-types/string.md))) — 应用行策略的角色和/或用户列表.

-    `apply_to_except` ([Array](../../sql-reference/data-types/array.md)([String](../../sql-reference/data-types/string.md))) — 行策略应用于除列出的角色和/或用户之外的所有角色和/或用户.

## 另请参阅 {#see-also}

-   [SHOW POLICIES](../../sql-reference/statements/show.md#show-policies-statement)

[原始文章](https://clickhouse.com/docs/en/operations/system-tables/row_policies) <!--hide-->
