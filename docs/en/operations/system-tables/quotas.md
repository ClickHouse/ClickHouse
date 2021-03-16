# system.quotas {#system_tables-quotas}

Contains information about [quotas](../../operations/system-tables/quotas.md).

Columns:
- `name` ([String](../../sql-reference/data-types/string.md)) — Quota name.
- `id` ([UUID](../../sql-reference/data-types/uuid.md)) — Quota ID.
- `storage`([String](../../sql-reference/data-types/string.md)) — Storage of quotas. Possible value: “users.xml” if a quota configured in the users.xml file, “disk” if a quota configured by an SQL-query.
- `keys` ([Array](../../sql-reference/data-types/array.md)([Enum8](../../sql-reference/data-types/enum.md))) — Key specifies how the quota should be shared. If two connections use the same quota and key, they share the same amounts of resources. Values:
- `[]` — All users share the same quota.
- `['user_name']` — Connections with the same user name share the same quota.
- `['ip_address']` — Connections from the same IP share the same quota.
- `['client_key']` — Connections with the same key share the same quota. A key must be explicitly provided by a client. When using [clickhouse-client](../../interfaces/cli.md), pass a key value in the `--quota-key` parameter, or use the `quota_key` parameter in the client configuration file. When using HTTP interface, use the `X-ClickHouse-Quota` header.
- `['user_name', 'client_key']` — Connections with the same `client_key` share the same quota. If a key isn’t provided by a client, the qouta is tracked for `user_name`.
- `['client_key', 'ip_address']` — Connections with the same `client_key` share the same quota. If a key isn’t provided by a client, the qouta is tracked for `ip_address`.
- `durations` ([Array](../../sql-reference/data-types/array.md)([UInt64](../../sql-reference/data-types/int-uint.md))) — Time interval lengths in seconds.
- `apply_to_all` ([UInt8](../../sql-reference/data-types/int-uint.md#uint-ranges)) — Logical value. It shows which users the quota is applied to. Values:
- `0` — The quota applies to users specify in the `apply_to_list`.
- `1` — The quota applies to all users except those listed in `apply_to_except`.
- `apply_to_list` ([Array](../../sql-reference/data-types/array.md)([String](../../sql-reference/data-types/string.md))) — List of user names/[roles](../../operations/access-rights.md#role-management) that the quota should be applied to.
- `apply_to_except` ([Array](../../sql-reference/data-types/array.md)([String](../../sql-reference/data-types/string.md))) — List of user names/roles that the quota should not apply to.

## See Also {#see-also}

-   [SHOW QUOTAS](../../sql-reference/statements/show.md#show-quotas-statement)

[Original article](https://clickhouse.tech/docs/en/operations/system_tables/quotas) <!--hide-->

