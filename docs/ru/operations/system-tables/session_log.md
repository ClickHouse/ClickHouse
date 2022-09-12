# system.session_log {#system_tables-session_log}

Содержит информацию о всех успешных и неудачных событиях входа и выхода из системы.

Столбцы:

-   `type` ([Enum8](../../sql-reference/data-types/enum.md)) — результат входа или выхода из системы. Возможные значения:
    -   `LoginFailure` — ошибка входа в систему.
    -   `LoginSuccess` — успешный вход в систему.
    -   `Logout` — выход из системы.
-   `auth_id` ([UUID](../../sql-reference/data-types/uuid.md)) — идентификатор аутентификации, представляющий собой UUID, который автоматически генерируется при каждом входе пользователя в систему.
-   `session_id` ([String](../../sql-reference/data-types/string.md)) — идентификатор сессии, который передается клиентом через [HTTP](../../interfaces/http.md)-интерфейс.
-   `event_date` ([Date](../../sql-reference/data-types/date.md)) — дата входа или выхода из системы.
-   `event_time` ([DateTime](../../sql-reference/data-types/datetime.md)) — время входа или выхода из системы.
-   `event_time_microseconds` ([DateTime64](../../sql-reference/data-types/datetime64.md)) — время начала входа или выхода из системы с точностью до микросекунд.
-   `user` ([String](../../sql-reference/data-types/string.md)) — имя пользователя.
-   `auth_type` ([Enum8](../../sql-reference/data-types/enum.md)) — тип аутентификации. Возможные значения:
    -   `NO_PASSWORD`
    -   `PLAINTEXT_PASSWORD`
    -   `SHA256_PASSWORD`
    -   `DOUBLE_SHA1_PASSWORD`
    -   `LDAP`
    -   `KERBEROS`
-   `profiles` ([Array](../../sql-reference/data-types/array.md)([LowCardinality(String)](../../sql-reference/data-types/lowcardinality.md))) — список профилей, установленных для всех ролей и (или) пользователей.
-   `roles` ([Array](../../sql-reference/data-types/array.md)([LowCardinality(String)](../../sql-reference/data-types/lowcardinality.md))) — список ролей, к которым применяется данный профиль.
-   `settings` ([Array](../../sql-reference/data-types/array.md)([Tuple](../../sql-reference/data-types/tuple.md)([LowCardinality(String)](../../sql-reference/data-types/lowcardinality.md), [String](../../sql-reference/data-types/string.md)))) — настройки, которые были изменены при входе или выходе клиента из системы.
-   `client_address` ([IPv6](../../sql-reference/data-types/domains/ipv6.md)) — IP-адрес, который использовался для входа или выхода из системы.
-   `client_port` ([UInt16](../../sql-reference/data-types/int-uint.md)) — порт клиента, который использовался для входа или выхода из системы.
-   `interface` ([Enum8](../../sql-reference/data-types/enum.md)) — интерфейс, с которого был инициирован вход в систему. Возможные значения:
    -   `TCP`
    -   `HTTP`
    -   `gRPC`
    -   `MySQL`
    -   `PostgreSQL`
-   `client_hostname` ([String](../../sql-reference/data-types/string.md)) — имя хоста клиентской машины, с которой присоединился [clickhouse-client](../../interfaces/cli.md) или другой TCP клиент.
-   `client_name` ([String](../../sql-reference/data-types/string.md)) — `clickhouse-client` или другой TCP клиент.
-   `client_revision` ([UInt32](../../sql-reference/data-types/int-uint.md)) — ревизия `clickhouse-client` или другого TCP клиента.
-   `client_version_major` ([UInt32](../../sql-reference/data-types/int-uint.md)) — старшая версия `clickhouse-client` или другого TCP клиента.
-   `client_version_minor` ([UInt32](../../sql-reference/data-types/int-uint.md)) — младшая версия `clickhouse-client` или другого TCP клиента.
-   `client_version_patch` ([UInt32](../../sql-reference/data-types/int-uint.md)) — патч `clickhouse-client` или другого TCP клиента.
-   `failure_reason` ([String](../../sql-reference/data-types/string.md)) — сообщение об исключении, содержащее причину сбоя при входе или выходе из системы.

**Пример**

Запрос:

``` sql
SELECT * FROM system.session_log LIMIT 1 FORMAT Vertical;
```

Результат:

``` text
Row 1:
──────
type:                    LoginSuccess
auth_id:                 45e6bd83-b4aa-4a23-85e6-bd83b4aa1a23
session_id:
event_date:              2021-10-14
event_time:              2021-10-14 20:33:52
event_time_microseconds: 2021-10-14 20:33:52.104247
user:                    default
auth_type:               PLAINTEXT_PASSWORD
profiles:                ['default']
roles:                   []
settings:                [('load_balancing','random'),('max_memory_usage','10000000000')]
client_address:          ::ffff:127.0.0.1
client_port:             38490
interface:               TCP
client_hostname:
client_name:             ClickHouse client
client_revision:         54449
client_version_major:    21
client_version_minor:    10
client_version_patch:    0
failure_reason:
```
