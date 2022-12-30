# system.quotas {#system_tables-quotas}

Содержит информацию о [квотах](quotas.md).

Столбцы:

-  `name` ([String](../../sql-reference/data-types/string.md)) — Имя квоты.
-   `id` ([UUID](../../sql-reference/data-types/uuid.md)) — ID квоты.
-   `storage`([String](../../sql-reference/data-types/string.md)) — Хранилище квот. Возможные значения: "users.xml", если квота задана в файле users.xml, "disk" — если квота задана в SQL-запросе.
-   `keys` ([Array](../../sql-reference/data-types/array.md)([Enum8](../../sql-reference/data-types/enum.md))) — Ключ определяет совместное использование квоты. Если два соединения используют одну и ту же квоту, они совместно используют один и тот же объем ресурсов. Значения:
    -   `[]` — Все пользователи используют одну и ту же квоту.
    -   `['user_name']` — Соединения с одинаковым именем пользователя используют одну и ту же квоту.
    -   `['ip_address']` — Соединения с одинаковым IP-адресом используют одну и ту же квоту.
    -   `['client_key']` — Соединения с одинаковым ключом используют одну и ту же квоту. Ключ может быть явно задан клиентом. При использовании [clickhouse-client](../../interfaces/cli.md), передайте ключевое значение в параметре `--quota_key`, или используйте параметр `quota_key` файле настроек клиента. В случае использования HTTP интерфейса, используйте заголовок `X-ClickHouse-Quota`.
    -   `['user_name', 'client_key']` — Соединения с одинаковым ключом используют одну и ту же квоту. Если ключ не предоставлен клиентом, то квота отслеживается для `user_name`.
    -   `['client_key', 'ip_address']` — Соединения с одинаковым ключом используют одну и ту же квоту. Если ключ не предоставлен клиентом, то квота отслеживается для `ip_address`.
-   `durations` ([Array](../../sql-reference/data-types/array.md)([UInt64](../../sql-reference/data-types/int-uint.md))) — Длины временных интервалов для расчета потребления ресурсов, в секундах.
-   `apply_to_all` ([UInt8](../../sql-reference/data-types/int-uint.md#uint-ranges)) — Логическое значение. Он показывает, к каким пользователям применяется квота. Значения:
    -   `0` — Квота применяется к пользователям, перечисленным в списке `apply_to_list`.
    -   `1` — Квота применяется к пользователям, за исключением тех, что перечислены в списке `apply_to_except`.
-   `apply_to_list` ([Array](../../sql-reference/data-types/array.md)([String](../../sql-reference/data-types/string.md))) — Список имен пользователей/[ролей](../../operations/access-rights.md#role-management) к которым применяется квота.
-   `apply_to_except` ([Array](../../sql-reference/data-types/array.md)([String](../../sql-reference/data-types/string.md))) — Список имен пользователей/ролей к которым квота применяться не должна.

## Смотрите также {#see-also}

-   [SHOW QUOTAS](../../sql-reference/statements/show.md#show-quotas-statement)


