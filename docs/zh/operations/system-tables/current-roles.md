# system.current_roles {#system_tables-current_roles}

包含当前用户的激活角色. `SET ROLE` 修改该表的内容.

列信息:

 - `role_name` ([String](../../sql-reference/data-types/string.md))) — 角色名称.
 - `with_admin_option` ([UInt8](../../sql-reference/data-types/int-uint.md#uint-ranges)) — 显示 `current_role` 是否是具有 `ADMIN OPTION` 权限的角色的标志.
 - `is_default` ([UInt8](../../sql-reference/data-types/int-uint.md#uint-ranges)) — 显示 `current_role` 是否为默认角色的标志.

 [原始文章](https://clickhouse.com/docs/en/operations/system-tables/current-roles) <!--hide-->
