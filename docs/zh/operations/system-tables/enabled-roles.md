# system.enabled_roles {#system_tables-enabled_roles}

包含当前所有活动角色, 包括当前用户的当前角色和当前角色的已授予角色.

列信息:

- `role_name` ([String](../../sql-reference/data-types/string.md))) — 角色名称.
- `with_admin_option` ([UInt8](../../sql-reference/data-types/int-uint.md#uint-ranges)) — 显示 `enabled_role` 是否为具有 `ADMIN OPTION` 权限的角色的标志.
- `is_current` ([UInt8](../../sql-reference/data-types/int-uint.md#uint-ranges)) — 显示  `enabled_role` 是否是当前用户的当前角色的标志.
- `is_default` ([UInt8](../../sql-reference/data-types/int-uint.md#uint-ranges)) — 显示 `enabled_role` 是否为默认角色的标志.

[原始文章](https://clickhouse.com/docs/en/operations/system-tables/enabled-roles) <!--hide-->
