#system.role_grants {#system_tables-role_grants}

包含用户和角色的角色授予. 向该表添加项, 请使用`GRANT role TO user`.

列信息:

- `user_name` ([Nullable](../../sql-reference/data-types/nullable.md)([String](../../sql-reference/data-types/string.md))) — 用户名称.

- `role_name` ([Nullable](../../sql-reference/data-types/nullable.md)([String](../../sql-reference/data-types/string.md))) — 角色名称.

- `granted_role_name` ([String](../../sql-reference/data-types/string.md)) — 授予 `role_name` 角色的角色名称. 要将一个角色授予另一个角色, 请使用`GRANT role1 TO role2`.

- `granted_role_is_default` ([UInt8](../../sql-reference/data-types/int-uint.md#uint-ranges)) — 显示 `granted_role` 是否为默认角色的标志. 参考值:
    -   1 — `granted_role` is a default role.
    -   0 — `granted_role` is not a default role.

- `with_admin_option` ([UInt8](../../sql-reference/data-types/int-uint.md#uint-ranges)) — 显示 `granted_role` 是否是具有 [ADMIN OPTION](../../sql-reference/statements/grant.md#admin-option-privilege) 特权的角色的标志. 参考值:
    -   1 — 该角色具有 `ADMIN OPTION` 权限.
    -   0 — 该角色不具有  `ADMIN OPTION` 权限.

[原始文章](https://clickhouse.com/docs/en/operations/system-tables/role-grants) <!--hide-->
