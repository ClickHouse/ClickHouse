# system.settings_profile_elements {#system_tables-settings_profile_elements}

描述settings配置文件的内容:

- 约束.
- setting适用的角色和用户.
- 父级 setting 配置文件.

列信息:
-    `profile_name` ([Nullable](../../sql-reference/data-types/nullable.md)([String](../../sql-reference/data-types/string.md))) — Setting 配置文件名称.

-    `user_name` ([Nullable](../../sql-reference/data-types/nullable.md)([String](../../sql-reference/data-types/string.md))) — 用户名称.

-    `role_name` ([Nullable](../../sql-reference/data-types/nullable.md)([String](../../sql-reference/data-types/string.md))) — 角色名称.

-    `index` ([UInt64](../../sql-reference/data-types/int-uint.md)) — settings 配置文件元素的顺序编号.

-    `setting_name` ([Nullable](../../sql-reference/data-types/nullable.md)([String](../../sql-reference/data-types/string.md))) — Setting 名称.

-    `value` ([Nullable](../../sql-reference/data-types/nullable.md)([String](../../sql-reference/data-types/string.md))) — Setting 值.

-    `min` ([Nullable](../../sql-reference/data-types/nullable.md)([String](../../sql-reference/data-types/string.md))) — setting 最小值. 未设置则赋 `NULL`.

-    `max` ([Nullable](../../sql-reference/data-types/nullable.md)([String](../../sql-reference/data-types/string.md))) — setting 最大值. 未设置则赋 `NULL`.

-    `readonly` ([Nullable](../../sql-reference/data-types/nullable.md)([UInt8](../../sql-reference/data-types/int-uint.md#uint-ranges))) — 只允许读查询的配置文件.

-    `inherit_profile` ([Nullable](../../sql-reference/data-types/nullable.md)([String](../../sql-reference/data-types/string.md))) — 此setting配置文件的父配置文件. 未设置则赋 `NULL`. 设置则将从其父配置文件继承所有设置的值和约束(`min`、`max`、`readonly`).

[原始文章](https://clickhouse.com/docs/en/operations/system-tables/settings_profile_elements) <!--hide-->
