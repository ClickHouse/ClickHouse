# system.query_thread_log {#system_tables-query_thread_log}

Содержит информацию о потоках, которые выполняют запросы, например, имя потока, время его запуска, продолжительность обработки запроса.

Чтобы начать логирование:

1. Настройте параметры [query_thread_log](../server-configuration-parameters/settings.md#server_configuration_parameters-query_thread_log) в конфигурации сервера.
2. Установите значение [log_query_threads](../settings/settings.md#settings-log-query-threads) равным 1.

Интервал сброса данных в таблицу задаётся параметром `flush_interval_milliseconds` в разделе настроек сервера [query_thread_log](../server-configuration-parameters/settings.md#server_configuration_parameters-query_thread_log). Чтобы принудительно записать логи из буфера памяти в таблицу, используйте запрос [SYSTEM FLUSH LOGS](../../sql-reference/statements/system.md#query_language-system-flush_logs).

ClickHouse не удаляет данные из таблицы автоматически. Подробности в разделе [Введение](../../operations/system-tables/index.md#system-tables-introduction).

Чтобы уменьшить количество запросов, регистрирующихся в таблице `query_thread_log`, вы можете использовать настройку [log_queries_probability](../../operations/settings/settings.md#log-queries-probability).

Столбцы:

-   `event_date` ([Date](../../sql-reference/data-types/date.md)) — дата завершения выполнения запроса потоком.
-   `event_time` ([DateTime](../../sql-reference/data-types/datetime.md)) — дата и время завершения выполнения запроса потоком.
-   `event_time_microseconds` ([DateTime](../../sql-reference/data-types/datetime.md)) — дата и время завершения выполнения запроса потоком с точностью до микросекунд.
-   `query_start_time` ([DateTime](../../sql-reference/data-types/datetime.md)) — время начала обработки запроса.
-   `query_start_time_microseconds` ([DateTime64](../../sql-reference/data-types/datetime64.md)) — время начала обработки запроса с точностью до микросекунд.
-   `query_duration_ms` ([UInt64](../../sql-reference/data-types/int-uint.md#uint-ranges)) — длительность обработки запроса в миллисекундах.
-   `read_rows` ([UInt64](../../sql-reference/data-types/int-uint.md#uint-ranges)) — количество прочитанных строк.
-   `read_bytes` ([UInt64](../../sql-reference/data-types/int-uint.md#uint-ranges)) — количество прочитанных байтов.
-   `written_rows` ([UInt64](../../sql-reference/data-types/int-uint.md#uint-ranges)) — количество записанных строк для запросов `INSERT`. Для других запросов, значение столбца 0.
-   `written_bytes` ([UInt64](../../sql-reference/data-types/int-uint.md#uint-ranges)) — объём записанных данных в байтах для запросов `INSERT`. Для других запросов, значение столбца 0.
-   `memory_usage` ([Int64](../../sql-reference/data-types/int-uint.md)) — разница между выделенной и освобождённой памятью в контексте потока.
-   `peak_memory_usage` ([Int64](../../sql-reference/data-types/int-uint.md)) — максимальная разница между выделенной и освобождённой памятью в контексте потока.
-   `thread_name` ([String](../../sql-reference/data-types/string.md)) — имя потока.
-   `thread_id` ([UInt64](../../sql-reference/data-types/int-uint.md#uint-ranges)) — tid (ID потока операционной системы).
-   `master_thread_id` ([UInt64](../../sql-reference/data-types/int-uint.md#uint-ranges)) — tid (ID потока операционной системы) главного потока.
-   `query` ([String](../../sql-reference/data-types/string.md)) — текст запроса.
-   `is_initial_query` ([UInt8](../../sql-reference/data-types/int-uint.md#uint-ranges)) — вид запроса. Возможные значения:
    -   1 — запрос был инициирован клиентом.
    -   0 — запрос был инициирован другим запросом при распределенном запросе.
-   `user` ([String](../../sql-reference/data-types/string.md)) — пользователь, запустивший текущий запрос.
-   `query_id` ([String](../../sql-reference/data-types/string.md)) — ID запроса.
-   `address` ([IPv6](../../sql-reference/data-types/domains/ipv6.md)) — IP адрес, с которого пришел запрос.
-   `port` ([UInt16](../../sql-reference/data-types/int-uint.md#uint-ranges)) — порт, с которого пришел запрос.
-   `initial_user` ([String](../../sql-reference/data-types/string.md)) — пользователь, запустивший первоначальный запрос (для распределенных запросов).
-   `initial_query_id` ([String](../../sql-reference/data-types/string.md)) — ID родительского запроса.
-   `initial_address` ([IPv6](../../sql-reference/data-types/domains/ipv6.md)) — IP адрес, с которого пришел родительский запрос.
-   `initial_port` ([UInt16](../../sql-reference/data-types/int-uint.md#uint-ranges)) — порт, пришел родительский запрос.
-   `interface` ([UInt8](../../sql-reference/data-types/int-uint.md#uint-ranges)) — интерфейс, с которого ушёл запрос. Возможные значения:
    -   1 — TCP.
    -   2 — HTTP.
-   `os_user` ([String](../../sql-reference/data-types/string.md)) — имя пользователя в OS, который запустил [clickhouse-client](../../interfaces/cli.md).
-   `client_hostname` ([String](../../sql-reference/data-types/string.md)) — hostname клиентской машины, с которой присоединился [clickhouse-client](../../interfaces/cli.md) или другой TCP клиент.
-   `client_name` ([String](../../sql-reference/data-types/string.md)) — [clickhouse-client](../../interfaces/cli.md) или другой TCP клиент.
-   `client_revision` ([UInt32](../../sql-reference/data-types/int-uint.md)) — ревизия [clickhouse-client](../../interfaces/cli.md) или другого TCP клиента.
-   `client_version_major` ([UInt32](../../sql-reference/data-types/int-uint.md)) — старшая версия [clickhouse-client](../../interfaces/cli.md) или другого TCP клиента.
-   `client_version_minor` ([UInt32](../../sql-reference/data-types/int-uint.md)) — младшая версия [clickhouse-client](../../interfaces/cli.md) или другого TCP клиента.
-   `client_version_patch` ([UInt32](../../sql-reference/data-types/int-uint.md)) — патч [clickhouse-client](../../interfaces/cli.md) или другого TCP клиента.
-   `http_method` ([UInt8](../../sql-reference/data-types/int-uint.md#uint-ranges)) — HTTP метод, инициировавший запрос. Возможные значения:
    -   0 — запрос запущен с интерфейса TCP.
    -   1 — `GET`.
    -   2 — `POST`.
-   `http_user_agent` ([String](../../sql-reference/data-types/string.md)) — HTTP заголовок `UserAgent`.
-   `quota_key` ([String](../../sql-reference/data-types/string.md)) — «ключ квоты» из настроек [квот](quotas.md) (см. `keyed`).
-   `revision` ([UInt32](../../sql-reference/data-types/int-uint.md)) — ревизия ClickHouse.
-   `ProfileEvents` ([Map(String, UInt64)](../../sql-reference/data-types/array.md)) — счетчики для изменения различных метрик для данного потока. Описание метрик можно получить из таблицы [system.events](#system_tables-events).

**Пример**

``` sql
 SELECT * FROM system.query_thread_log LIMIT 1 \G
```

``` text
Row 1:
──────
event_date:                    2020-09-11
event_time:                    2020-09-11 10:08:17
event_time_microseconds:       2020-09-11 10:08:17.134042
query_start_time:              2020-09-11 10:08:17
query_start_time_microseconds: 2020-09-11 10:08:17.063150
query_duration_ms:             70
read_rows:                     0
read_bytes:                    0
written_rows:                  1
written_bytes:                 12
memory_usage:                  4300844
peak_memory_usage:             4300844
thread_name:                   TCPHandler
thread_id:                     638133
master_thread_id:              638133
query:                         INSERT INTO test1 VALUES
is_initial_query:              1
user:                          default
query_id:                      50a320fd-85a8-49b8-8761-98a86bcbacef
address:                       ::ffff:127.0.0.1
port:                          33452
initial_user:                  default
initial_query_id:              50a320fd-85a8-49b8-8761-98a86bcbacef
initial_address:               ::ffff:127.0.0.1
initial_port:                  33452
interface:                     1
os_user:                       bharatnc
client_hostname:               tower
client_name:                   ClickHouse
client_revision:               54437
client_version_major:          20
client_version_minor:          7
client_version_patch:          2
http_method:                   0
http_user_agent:
quota_key:
revision:                      54440
ProfileEvents:        {'Query':1,'SelectQuery':1,'ReadCompressedBytes':36,'CompressedReadBufferBlocks':1,'CompressedReadBufferBytes':10,'IOBufferAllocs':1,'IOBufferAllocBytes':89,'ContextLock':15,'RWLockAcquiredReadLocks':1}
```

**Смотрите также**

-   [system.query_log](../../operations/system-tables/query_log.md#system_tables-query_log) — описание системной таблицы `query_log`, которая содержит общую информацию о выполненных запросах.
-   [system.query_views_log](../../operations/system-tables/query_views_log.md#system_tables-query_views_log) — описание системной таблицы `query_views_log`, которая содержит информацию о всех представлениях, участвующих в выполненных запросах.
