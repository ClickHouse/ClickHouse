# system.roles {#system_tables-roles}

包含有关已配置的 [角色](../../operations/access-rights.md#role-management) 信息.

列信息:

- `name` ([String](../../sql-reference/data-types/string.md)) — 角色名称.
- `id` ([UUID](../../sql-reference/data-types/uuid.md)) — 角色 ID.
- `storage` ([String](../../sql-reference/data-types/string.md)) — 角色存储的路径. 在 `access_control_path` 参数中配置.

## 另请参阅 {#see-also}

-   [查看角色信息](../../sql-reference/statements/show.md#show-roles-statement)

[原始文章](https://clickhouse.com/docs/en/operations/system-tables/roles) <!--hide-->
