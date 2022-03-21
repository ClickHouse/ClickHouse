# system.users {#system_tables-users}

包含服务器上配置的[用户账号](../../operations/access-rights.md#user-account-management)的列表.

列信息:
-    `name` ([String](../../sql-reference/data-types/string.md)) — 用户名称.

-    `id` ([UUID](../../sql-reference/data-types/uuid.md)) — 用户 ID.

-    `storage` ([String](../../sql-reference/data-types/string.md)) — 用户存储路径. 在 `access_control_path` 参数中配置.

-    `auth_type` ([Enum8](../../sql-reference/data-types/enum.md)('no_password' = 0,'plaintext_password' = 1, 'sha256_password' = 2, 'double_sha1_password' = 3)) — 显示认证类型. 有多种用户识别方式: 无密码, 纯文本密码, [SHA256](https://ru.wikipedia.org/wiki/SHA-2)-encoded password or with [double SHA-1](https://ru.wikipedia.org/wiki/SHA-1)-编码的密码.

-    `auth_params` ([String](../../sql-reference/data-types/string.md)) — JSON 格式的身份验证参数取决于`auth_type`.

-    `host_ip` ([Array](../../sql-reference/data-types/array.md)([String](../../sql-reference/data-types/string.md))) — 许连接到 ClickHouse 服务器的主机的 IP 地址.

-    `host_names` ([Array](../../sql-reference/data-types/array.md)([String](../../sql-reference/data-types/string.md))) — 允许连接到 ClickHouse 服务器的主机名称.

-    `host_names_regexp` ([Array](../../sql-reference/data-types/array.md)([String](../../sql-reference/data-types/string.md))) — 允许连接到 ClickHouse 服务器的主机名的正则表达式.

-    `host_names_like` ([Array](../../sql-reference/data-types/array.md)([String](../../sql-reference/data-types/string.md))) — 使用 LIKE 谓词设置允许连接到 ClickHouse 服务器的主机名称.

-    `default_roles_all` ([UInt8](../../sql-reference/data-types/int-uint.md#uint-ranges)) — 显示默认情况下为用户设置的所有授予的角色.

-    `default_roles_list` ([Array](../../sql-reference/data-types/array.md)([String](../../sql-reference/data-types/string.md))) — 默认提供的授权角色列表.

-    `default_roles_except` ([Array](../../sql-reference/data-types/array.md)([String](../../sql-reference/data-types/string.md))) — 除了列出的角色之外所有授予的角色都设置为默认值.

## 另请参阅 {#see-also}

-   [查看用户信息](../../sql-reference/statements/show.md#show-users-statement)

[原始文章](https://clickhouse.com/docs/en/operations/system-tables/users) <!--hide-->
