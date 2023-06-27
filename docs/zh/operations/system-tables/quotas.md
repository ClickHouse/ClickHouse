# system.quotas {#system_tables-quotas}

包含 [quotas](../../operations/system-tables/quotas.md) 信息.

列信息:
- `name` ([String](../../sql-reference/data-types/string.md)) — 配额名称.
- `id` ([UUID](../../sql-reference/data-types/uuid.md)) — 配额 ID.
- `storage`([String](../../sql-reference/data-types/string.md)) — 存储配额. 可能的值：“users.xml”(如果在 users.xml 文件中配置了配额), “disk”(如果由 SQL 查询配置的配额).
- `keys` ([Array](../../sql-reference/data-types/array.md)([Enum8](../../sql-reference/data-types/enum.md))) — Key指定配额应该如何共享. 如果两个连接使用相同的配额和键，则它们共享相同数量的资源. 值:
    - `[]` — 所有用户共享相同的配额.
    - `['user_name']` — 相同用户名的连接共享相同的配额.
    - `['ip_address']` — 来自同一IP的连接共享相同的配额.
    - `['client_key']` — 具有相同密钥的连接共享相同配额. 密钥必须由客户端显式提供. 使用[clickhouse-client](../../interfaces/cli.md)时, 在 `--quota_key` 参数中传递一个key值, 或者在客户端配置文件中使用 `quota_key` 参数. 使用 HTTP 接口时, 使用 `X-ClickHouse-Quota` 报头.
    - `['user_name', 'client_key']` — 具有相同 `client_key` 的连接共享相同的配额. 如果客户端没有提供密钥, 配额将跟踪 `user_name`.
    - `['client_key', 'ip_address']` — 具有相同 `client_key` 的连接共享相同的配额. 如果客户端没有提供密钥, 配额将跟踪 `ip_address`.
- `durations` ([Array](../../sql-reference/data-types/array.md)([UInt64](../../sql-reference/data-types/int-uint.md))) — 时间间隔以秒为单位.
- `apply_to_all` ([UInt8](../../sql-reference/data-types/int-uint.md#uint-ranges)) — 逻辑值. 它显示了配额应用于哪些用户. 值:
    - `0` — 配额应用于 `apply_to_list` 中指定的用户.
    - `1` — 配额适用于除 `apply_to_except` 中列出的用户之外的所有用户.
- `apply_to_list` ([Array](../../sql-reference/data-types/array.md)([String](../../sql-reference/data-types/string.md))) — 需要应用配额的用户名/[角色](../../operations/access-rights.md#role-management) 列表.
- `apply_to_except` ([Array](../../sql-reference/data-types/array.md)([String](../../sql-reference/data-types/string.md))) — 不需要应用配额的用户名/角色列表.

## 另请参阅 {#see-also}

-   [查看配额信息](../../sql-reference/statements/show.md#show-quotas-statement)

[原始文章](https://clickhouse.com/docs/en/operations/system-tables/quotas) <!--hide-->

