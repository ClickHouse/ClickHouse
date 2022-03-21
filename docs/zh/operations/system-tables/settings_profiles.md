# system.settings_profiles {#system_tables-settings_profiles}

包含 Setting 配置文件中指定的属性.

列信息:
-    `name` ([String](../../sql-reference/data-types/string.md)) — Setting 配置文件 name.

-    `id` ([UUID](../../sql-reference/data-types/uuid.md)) — Setting 配置文件 ID.

-    `storage` ([String](../../sql-reference/data-types/string.md)) — Setting 配置文件的存储路径. 在`access_control_path`参数中配置.

-    `num_elements` ([UInt64](../../sql-reference/data-types/int-uint.md)) — `system.settings_profile_elements` 表中此配置文件的元素数.

-    `apply_to_all` ([UInt8](../../sql-reference/data-types/int-uint.md#uint-ranges)) — 为所有角色和/或用户设置的 Setting 配置文件.

-    `apply_to_list` ([Array](../../sql-reference/data-types/array.md)([String](../../sql-reference/data-types/string.md))) — 应用 Setting 配置文件的角色和/或用户列表. 

-    `apply_to_except` ([Array](../../sql-reference/data-types/array.md)([String](../../sql-reference/data-types/string.md))) — Setting 配置文件适用于除所列角色和/或用户之外的所有角色和/或用户.

## 另请参阅 {#see-also}

-   [查看配置文件信息](../../sql-reference/statements/show.md#show-profiles-statement)

[原始文章](https://clickhouse.com/docs/en/operations/system-tables/settings_profiles) <!--hide-->
