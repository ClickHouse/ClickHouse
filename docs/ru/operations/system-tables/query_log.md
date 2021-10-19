# system.query_log {#system_tables-query_log}

Содержит информацию о выполняемых запросах, например, время начала обработки, продолжительность обработки, сообщения об ошибках.

!!! note "Внимание"
    Таблица не содержит входных данных для запросов `INSERT`.

Настойки логгирования можно изменить в секции серверной конфигурации [query_log](../server-configuration-parameters/settings.md#server_configuration_parameters-query-log).

Можно отключить логгирование настройкой [log_queries = 0](../settings/settings.md#settings-log-queries). По-возможности, не отключайте логгирование, поскольку информация из таблицы важна при решении проблем.

Период сброса данных в таблицу задаётся параметром `flush_interval_milliseconds` в конфигурационной секции [query_log](../server-configuration-parameters/settings.md#server_configuration_parameters-query-log). Чтобы принудительно записать логи из буффера памяти в таблицу, используйте запрос [SYSTEM FLUSH LOGS](../../sql-reference/statements/system.md#query_language-system-flush_logs).

ClickHouse не удаляет данные из таблица автоматически. Смотрите [Введение](#system-tables-introduction).

Таблица `system.query_log` содержит информацию о двух видах запросов:

1.  Первоначальные запросы, которые были выполнены непосредственно клиентом.
2.  Дочерние запросы, инициированные другими запросами (для выполнения распределенных запросов). Для дочерних запросов информация о первоначальном запросе содержится в столбцах `initial_*`.

В зависимости от статуса (столбец `type`) каждый запрос создаёт одну или две строки в таблице `query_log`:

1.  Если запрос выполнен успешно, создаются два события типа `QueryStart` и `QueryFinish`.
2.  Если во время обработки запроса возникла ошибка, создаются два события с типами `QueryStart` и `ExceptionWhileProcessing`.
3.  Если ошибка произошла ещё до запуска запроса, создается одно событие с типом `ExceptionBeforeStart`.

Чтобы уменьшить количество запросов, регистрирующихся в таблице `query_log`, вы можете использовать настройку [log_queries_probability](../../operations/settings/settings.md#log-queries-probability).

Столбцы:

-   `type` ([Enum8](../../sql-reference/data-types/enum.md)) — тип события, произошедшего при выполнении запроса. Значения:
    -   `'QueryStart' = 1` — успешное начало выполнения запроса.
    -   `'QueryFinish' = 2` — успешное завершение выполнения запроса.
    -   `'ExceptionBeforeStart' = 3` — исключение перед началом обработки запроса.
    -   `'ExceptionWhileProcessing' = 4` — исключение во время обработки запроса.
-   `event_date` ([Date](../../sql-reference/data-types/date.md)) — дата начала запроса.
-   `event_time` ([DateTime](../../sql-reference/data-types/datetime.md)) — время начала запроса.
-   `event_time_microseconds` ([DateTime](../../sql-reference/data-types/datetime.md)) — время начала запроса с точностью до микросекунд.
-   `query_start_time` ([DateTime](../../sql-reference/data-types/datetime.md)) — время начала обработки запроса.
-   `query_start_time_microseconds` ([DateTime64](../../sql-reference/data-types/datetime64.md)) — время начала обработки запроса с точностью до микросекунд.
-   `query_duration_ms` ([UInt64](../../sql-reference/data-types/int-uint.md#uint-ranges)) — длительность выполнения запроса в миллисекундах.
-   `read_rows` ([UInt64](../../sql-reference/data-types/int-uint.md#uint-ranges)) — общее количество строк, считанных из всех таблиц и табличных функций, участвующих в запросе. Включает в себя обычные подзапросы, подзапросы для `IN` и `JOIN`. Для распределенных запросов `read_rows` включает в себя общее количество строк, прочитанных на всех репликах. Каждая реплика передает собственное значение `read_rows`, а сервер-инициатор запроса суммирует все полученные и локальные значения. Объемы кэша не учитываюся.
-   `read_bytes` ([UInt64](../../sql-reference/data-types/int-uint.md#uint-ranges)) — общее количество байтов, считанных из всех таблиц и табличных функций, участвующих в запросе. Включает в себя обычные подзапросы, подзапросы для `IN` и `JOIN`. Для распределенных запросов `read_bytes` включает в себя общее количество байтов, прочитанных на всех репликах. Каждая реплика передает собственное значение `read_bytes`, а сервер-инициатор запроса суммирует все полученные и локальные значения. Объемы кэша не учитываюся.
-   `written_rows` ([UInt64](../../sql-reference/data-types/int-uint.md#uint-ranges)) — количество записанных строк для запросов `INSERT`. Для других запросов, значение столбца 0.
-   `written_bytes` ([UInt64](../../sql-reference/data-types/int-uint.md#uint-ranges)) — объём записанных данных в байтах для запросов `INSERT`. Для других запросов, значение столбца 0.
-   `result_rows` ([UInt64](../../sql-reference/data-types/int-uint.md#uint-ranges)) — количество строк в результате запроса `SELECT` или количество строк в запросе `INSERT`.
-   `result_bytes` ([UInt64](../../sql-reference/data-types/int-uint.md#uint-ranges)) — объём RAM в байтах, использованный для хранения результата запроса.
-   `memory_usage` ([UInt64](../../sql-reference/data-types/int-uint.md#uint-ranges)) — потребление RAM запросом.
-   `current_database` ([String](../../sql-reference/data-types/string.md)) — имя текущей базы данных.
-   `query` ([String](../../sql-reference/data-types/string.md)) — текст запроса.
-   `normalized_query_hash` ([UInt64](../../sql-reference/data-types/int-uint.md#uint-ranges)) — идентичная хэш-сумма без значений литералов для аналогичных запросов.
-   `query_kind` ([LowCardinality(String)](../../sql-reference/data-types/lowcardinality.md)) — тип запроса.
-   `databases` ([Array](../../sql-reference/data-types/array.md)([LowCardinality(String)](../../sql-reference/data-types/lowcardinality.md))) — имена баз данных, присутствующих в запросе.
-   `tables` ([Array](../../sql-reference/data-types/array.md)([LowCardinality(String)](../../sql-reference/data-types/lowcardinality.md))) — имена таблиц, присутствующих в запросе.
-   `columns` ([Array](../../sql-reference/data-types/array.md)([LowCardinality(String)](../../sql-reference/data-types/lowcardinality.md))) — имена столбцов, присутствующих в запросе.
-   `projections` ([String](../../sql-reference/data-types/string.md)) — имена проекций, использованных при выполнении запроса.
-   `exception_code` ([Int32](../../sql-reference/data-types/int-uint.md)) — код исключения.
-   `exception` ([String](../../sql-reference/data-types/string.md)) — сообщение исключения, если запрос завершился по исключению.
-   `stack_trace` ([String](../../sql-reference/data-types/string.md)) — [stack trace](https://en.wikipedia.org/wiki/Stack_trace). Пустая строка, если запрос успешно завершен.
-   `is_initial_query` ([UInt8](../../sql-reference/data-types/int-uint.md)) — вид запроса. Возможные значения:
    -   1 — запрос был инициирован клиентом.
    -   0 — запрос был инициирован другим запросом при выполнении распределенного запроса.
-   `user` ([String](../../sql-reference/data-types/string.md)) — пользователь, запустивший текущий запрос.
-   `query_id` ([String](../../sql-reference/data-types/string.md)) — ID запроса.
-   `address` ([IPv6](../../sql-reference/data-types/domains/ipv6.md)) — IP адрес, с которого пришел запрос.
-   `port` ([UInt16](../../sql-reference/data-types/int-uint.md)) — порт, с которого клиент сделал запрос
-   `initial_user` ([String](../../sql-reference/data-types/string.md)) — пользователь, запустивший первоначальный запрос (для распределенных запросов).
-   `initial_query_id` ([String](../../sql-reference/data-types/string.md)) — ID родительского запроса.
-   `initial_address` ([IPv6](../../sql-reference/data-types/domains/ipv6.md)) — IP адрес, с которого пришел родительский запрос.
-   `initial_port` ([UInt16](../../sql-reference/data-types/int-uint.md)) — порт, с которого клиент сделал родительский запрос.
-   `initial_query_start_time` ([DateTime](../../sql-reference/data-types/datetime.md)) — время начала обработки запроса (для распределенных запросов).
-   `initial_query_start_time_microseconds` ([DateTime64](../../sql-reference/data-types/datetime64.md)) — время начала обработки запроса с точностью до микросекунд (для распределенных запросов).
-   `interface` ([UInt8](../../sql-reference/data-types/int-uint.md)) — интерфейс, с которого ушёл запрос. Возможные значения:
    -   1 — TCP.
    -   2 — HTTP.
-   `os_user` ([String](../../sql-reference/data-types/string.md)) — имя пользователя операционной системы, который запустил [clickhouse-client](../../interfaces/cli.md).
-   `client_hostname` ([String](../../sql-reference/data-types/string.md)) — имя сервера, с которого присоединился [clickhouse-client](../../interfaces/cli.md) или другой TCP клиент.
-   `client_name` ([String](../../sql-reference/data-types/string.md)) — [clickhouse-client](../../interfaces/cli.md) или другой TCP клиент.
-   `client_revision` ([UInt32](../../sql-reference/data-types/int-uint.md)) — ревизия [clickhouse-client](../../interfaces/cli.md) или другого TCP клиента.
-   `client_version_major` ([UInt32](../../sql-reference/data-types/int-uint.md)) — старшая версия [clickhouse-client](../../interfaces/cli.md) или другого TCP клиента.
-   `client_version_minor` ([UInt32](../../sql-reference/data-types/int-uint.md)) — младшая версия [clickhouse-client](../../interfaces/cli.md) или другого TCP клиента.
-   `client_version_patch` ([UInt32](../../sql-reference/data-types/int-uint.md)) — патч [clickhouse-client](../../interfaces/cli.md) или другого TCP клиента.
-   `http_method` ([UInt8](../../sql-reference/data-types/int-uint.md)) — HTTP метод, инициировавший запрос. Возможные значения:
    -   0 — запрос запущен с интерфейса TCP.
    -   1 — `GET`.
    -   2 — `POST`.
-   `http_user_agent` ([String](../../sql-reference/data-types/string.md)) — HTTP заголовок `UserAgent`.
-   `http_referer` ([String](../../sql-reference/data-types/string.md)) — HTTP заголовок `Referer` (содержит полный или частичный адрес страницы, с которой был выполнен запрос).
-   `forwarded_for` ([String](../../sql-reference/data-types/string.md)) — HTTP заголовок `X-Forwarded-For`.
-   `quota_key` ([String](../../sql-reference/data-types/string.md)) — `ключ квоты` из настроек [квот](quotas.md) (см. `keyed`).
-   `revision` ([UInt32](../../sql-reference/data-types/int-uint.md)) — ревизия ClickHouse.
-   `ProfileEvents` ([Map(String, UInt64)](../../sql-reference/data-types/array.md)) — счетчики для изменения различных метрик. Описание метрик можно получить из таблицы [system.events](#system_tables-events)(#system_tables-events
-   `Settings` ([Map(String, String)](../../sql-reference/data-types/array.md)) — имена настроек, которые меняются, когда клиент выполняет запрос. Чтобы разрешить логирование изменений настроек, установите параметр `log_query_settings` равным 1.
-   `log_comment` ([String](../../sql-reference/data-types/string.md)) — комментарий к записи в логе. Представляет собой произвольную строку, длина которой должна быть не больше, чем [max_query_size](../../operations/settings/settings.md#settings-max_query_size). Если нет комментария, то пустая строка.
-   `thread_ids` ([Array(UInt64)](../../sql-reference/data-types/array.md)) — идентификаторы потоков, участвующих в обработке запросов.
-   `used_aggregate_functions` ([Array(String)](../../sql-reference/data-types/array.md)) — канонические имена `агрегатных функций`, использованных при выполнении запроса.
-   `used_aggregate_function_combinators` ([Array(String)](../../sql-reference/data-types/array.md)) — канонические имена `комбинаторов агрегатных функций`, использованных при выполнении запроса.
-   `used_database_engines` ([Array(String)](../../sql-reference/data-types/array.md)) — канонические имена `движков баз данных`, использованных при выполнении запроса.
-   `used_data_type_families` ([Array(String)](../../sql-reference/data-types/array.md)) — канонические имена `семейств типов данных`, использованных при выполнении запроса.
-   `used_dictionaries` ([Array(String)](../../sql-reference/data-types/array.md)) — канонические имена `источников словарей`, использованных при выполнении запроса.
-   `used_formats` ([Array(String)](../../sql-reference/data-types/array.md)) — канонические имена `форматов`, использованных при выполнении запроса.
-   `used_functions` ([Array(String)](../../sql-reference/data-types/array.md)) — канонические имена `функций`, использованных при выполнении запроса.
-   `used_storages` ([Array(String)](../../sql-reference/data-types/array.md)) — канонические имена `движков таблиц`, использованных при выполнении запроса.
-   `used_table_functions` ([Array(String)](../../sql-reference/data-types/array.md)) — канонические имена `табличных функций`, использованных при выполнении запроса.

**Пример**

``` sql
SELECT * FROM system.query_log WHERE type = 'QueryFinish' ORDER BY query_start_time DESC LIMIT 1 FORMAT Vertical;
```

``` text
Row 1:
──────
type:                                  QueryFinish
event_date:                            2021-07-28
event_time:                            2021-07-28 13:46:56
event_time_microseconds:               2021-07-28 13:46:56.719791
query_start_time:                      2021-07-28 13:46:56
query_start_time_microseconds:         2021-07-28 13:46:56.704542
query_duration_ms:                     14
read_rows:                             8393
read_bytes:                            374325
written_rows:                          0
written_bytes:                         0
result_rows:                           4201
result_bytes:                          153024
memory_usage:                          4714038
current_database:                      default
query:                                 SELECT DISTINCT arrayJoin(extractAll(name, '[\\w_]{2,}')) AS res FROM (SELECT name FROM system.functions UNION ALL SELECT name FROM system.table_engines UNION ALL SELECT name FROM system.formats UNION ALL SELECT name FROM system.table_functions UNION ALL SELECT name FROM system.data_type_families UNION ALL SELECT name FROM system.merge_tree_settings UNION ALL SELECT name FROM system.settings UNION ALL SELECT cluster FROM system.clusters UNION ALL SELECT macro FROM system.macros UNION ALL SELECT policy_name FROM system.storage_policies UNION ALL SELECT concat(func.name, comb.name) FROM system.functions AS func CROSS JOIN system.aggregate_function_combinators AS comb WHERE is_aggregate UNION ALL SELECT name FROM system.databases LIMIT 10000 UNION ALL SELECT DISTINCT name FROM system.tables LIMIT 10000 UNION ALL SELECT DISTINCT name FROM system.dictionaries LIMIT 10000 UNION ALL SELECT DISTINCT name FROM system.columns LIMIT 10000) WHERE notEmpty(res)
normalized_query_hash:                 6666026786019643712
query_kind:                            Select
databases:                             ['system']
tables:                                ['system.aggregate_function_combinators','system.clusters','system.columns','system.data_type_families','system.databases','system.dictionaries','system.formats','system.functions','system.macros','system.merge_tree_settings','system.settings','system.storage_policies','system.table_engines','system.table_functions','system.tables']
columns:                               ['system.aggregate_function_combinators.name','system.clusters.cluster','system.columns.name','system.data_type_families.name','system.databases.name','system.dictionaries.name','system.formats.name','system.functions.is_aggregate','system.functions.name','system.macros.macro','system.merge_tree_settings.name','system.settings.name','system.storage_policies.policy_name','system.table_engines.name','system.table_functions.name','system.tables.name']
projections:                           []
exception_code:                        0
exception:
stack_trace:
is_initial_query:                      1
user:                                  default
query_id:                              a3361f6e-a1fd-4d54-9f6f-f93a08bab0bf
address:                               ::ffff:127.0.0.1
port:                                  51006
initial_user:                          default
initial_query_id:                      a3361f6e-a1fd-4d54-9f6f-f93a08bab0bf
initial_address:                       ::ffff:127.0.0.1
initial_port:                          51006
initial_query_start_time:              2021-07-28 13:46:56
initial_query_start_time_microseconds: 2021-07-28 13:46:56.704542
interface:                             1
os_user:
client_hostname:
client_name:                           ClickHouse client
client_revision:                       54449
client_version_major:                  21
client_version_minor:                  8
client_version_patch:                  0
http_method:                           0
http_user_agent:
http_referer:
forwarded_for:
quota_key:
revision:                              54453
log_comment:
thread_ids:                            [5058,22097,22110,22094]
ProfileEvents.Names:                   ['Query','SelectQuery','ArenaAllocChunks','ArenaAllocBytes','FunctionExecute','NetworkSendElapsedMicroseconds','SelectedRows','SelectedBytes','ContextLock','RWLockAcquiredReadLocks','RealTimeMicroseconds','UserTimeMicroseconds','SystemTimeMicroseconds','SoftPageFaults','OSCPUWaitMicroseconds','OSCPUVirtualTimeMicroseconds','OSWriteBytes','OSWriteChars']
ProfileEvents.Values:                  [1,1,39,352256,64,360,8393,374325,412,440,34480,13108,4723,671,19,17828,8192,10240]
Settings.Names:                        ['load_balancing','max_memory_usage']
Settings.Values:                       ['random','10000000000']
used_aggregate_functions:              []
used_aggregate_function_combinators:   []
used_database_engines:                 []
used_data_type_families:               ['UInt64','UInt8','Nullable','String','date']
used_dictionaries:                     []
used_formats:                          []
used_functions:                        ['concat','notEmpty','extractAll']
used_storages:                         []
used_table_functions:                  []
```

**Смотрите также**

-   [system.query_thread_log](../../operations/system-tables/query_thread_log.md#system_tables-query_thread_log) — в этой таблице содержится информация о цепочке каждого выполненного запроса.

[Оригинальная статья](https://clickhouse.com/docs/ru/operations/system_tables/query_log) <!--hide-->
