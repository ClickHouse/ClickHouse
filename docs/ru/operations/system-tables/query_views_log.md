# system.query_views_log {#system_tables-query_views_log}

Содержит информацию о зависимых представлениях, выполняемых при выполнении запроса, например, тип представления или время выполнения.

Чтобы начать ведение журнала:

1. Настройте параметры в разделе [query_views_log](../../operations/server-configuration-parameters/settings.md#server_configuration_parameters-query_views_log).
2. Включите настройку [log_query_views=1](../../operations/settings/settings.md#settings-log-query-views).

Период сброса данных из буфера в памяти задается в параметре `flush_interval_milliseconds` в разделе настроек сервера [query_views_log](../../operations/server-configuration-parameters/settings.md#server_configuration_parameters-query_views_log ). Для принудительного сброса используйте запрос [SYSTEM FLUSH LOGS](../../sql-reference/statements/system.md#query_language-system-flush_logs).

ClickHouse не удаляет данные из таблицы автоматически. Подробнее смотрите раздел [Системные таблицы](../../operations/system-tables/index.md#system-tables-introduction).

Чтобы уменьшить количество запросов, регистрируемых в таблице `query_views_log`, вы можете включить настройку [log_queries_probability](../../operations/settings/settings.md#log-queries-probability).

Столбцы:

-   `event_date` ([Date](../../sql-reference/data-types/date.md)) — дата, когда произошло последнее событие с представлением.
-   `event_time` ([DateTime](../../sql-reference/data-types/datetime.md)) — дата и время завершения выполнения представления.
-   `event_time_microseconds` ([DateTime](../../sql-reference/data-types/datetime.md)) — дата и время завершения выполнения представления с точностью до микросекунд.
-   `view_duration_ms` ([UInt64](../../sql-reference/data-types/int-uint.md#uint-ranges)) — продолжительность выполнения представления (сумма его этапов) в миллисекундах.
-   `initial_query_id` ([String](../../sql-reference/data-types/string.md)) — идентификатор начального запроса (при распределённом выполнении запроса).
-   `view_name` ([String](../../sql-reference/data-types/string.md)) — имя представления.
-   `view_uuid` ([UUID](../../sql-reference/data-types/uuid.md)) — UUID представления.
-   `view_type` ([Enum8](../../sql-reference/data-types/enum.md)) — тип представления. Возможные значения:
    -   `'Default' = 1` — [обычные представления](../../sql-reference/statements/create/view.md#normal). Не должно появляться в этом журнале.
    -   `'Materialized' = 2` — [материализованные представления](../../sql-reference/statements/create/view.md#materialized).
    -   `'Live' = 3` — [live представления](../../sql-reference/statements/create/view.md#live-view).
-   `view_query` ([String](../../sql-reference/data-types/string.md)) — запрос, выполняемый представлением.
-   `view_target` ([String](../../sql-reference/data-types/string.md)) — имя целевой таблицы представления.
-   `read_rows` ([UInt64](../../sql-reference/data-types/int-uint.md#uint-ranges)) — количество прочитанных строк.
-   `read_bytes` ([UInt64](../../sql-reference/data-types/int-uint.md#uint-ranges)) — количество прочитанных байт.
-   `written_rows` ([UInt64](../../sql-reference/data-types/int-uint.md#uint-ranges)) — количество записанных строк.
-   `written_bytes` ([UInt64](../../sql-reference/data-types/int-uint.md#uint-ranges)) — количество записанных байт.
-   `peak_memory_usage` ([Int64](../../sql-reference/data-types/int-uint.md)) — максимальная разница между объемом выделенной и освобожденной памяти в контексте этого представления.
-   `ProfileEvents` ([Map(String, UInt64)](../../sql-reference/data-types/array.md)) — события профиля, которые измеряют различные показатели. Их описание можно найти в таблице [system.events](../../operations/system-tables/events.md#system_tables-events).
-   `status` ([Enum8](../../sql-reference/data-types/enum.md)) — статус представления. Возможные значения:
    -   `'QueryStart' = 1` — успешное начало выполнения представления. Не должно отображаться.
    -   `'QueryFinish' = 2` — успешное завершение выполнения представления.
    -   `'ExceptionBeforeStart' = 3` — исключение до начала выполнения представления.
    -   `'ExceptionWhileProcessing' = 4` — исключение во время выполнения представления.
-   `exception_code` ([Int32](../../sql-reference/data-types/int-uint.md)) — код исключения.
-   `exception` ([String](../../sql-reference/data-types/string.md)) — сообщение исключения.
-   `stack_trace` ([String](../../sql-reference/data-types/string.md)) — [трассировка стека](https://ru.wikipedia.org/wiki/Трассировка_стека). Пустая строка, если запрос был успешно выполнен.

**Пример**

Запрос:

``` sql
SELECT * FROM system.query_views_log LIMIT 1 \G;
```

Результат:

``` text
Row 1:
──────
event_date:              2021-06-22
event_time:              2021-06-22 13:23:07
event_time_microseconds: 2021-06-22 13:23:07.738221
view_duration_ms:        0
initial_query_id:        c3a1ac02-9cad-479b-af54-9e9c0a7afd70
view_name:               default.matview_inner
view_uuid:               00000000-0000-0000-0000-000000000000
view_type:               Materialized
view_query:              SELECT * FROM default.table_b
view_target:             default.`.inner.matview_inner`
read_rows:               4
read_bytes:              64
written_rows:            2
written_bytes:           32
peak_memory_usage:       4196188
ProfileEvents:           {'FileOpen':2,'WriteBufferFromFileDescriptorWrite':2,'WriteBufferFromFileDescriptorWriteBytes':187,'IOBufferAllocs':3,'IOBufferAllocBytes':3145773,'FunctionExecute':3,'DiskWriteElapsedMicroseconds':13,'InsertedRows':2,'InsertedBytes':16,'SelectedRows':4,'SelectedBytes':48,'ContextLock':16,'RWLockAcquiredReadLocks':1,'RealTimeMicroseconds':698,'SoftPageFaults':4,'OSReadChars':463}
status:                  QueryFinish
exception_code:          0
exception:
stack_trace:
```

**См. также**

-   [system.query_log](../../operations/system-tables/query_log.md#system_tables-query_log) — описание системной таблицы `query_log`, которая содержит общую информацию о выполненных запросах.
-   [system.query_thread_log](../../operations/system-tables/query_thread_log.md#system_tables-query_thread_log) — описание системной таблицы `query_thread_log`, которая содержит информацию о каждом потоке выполнения запроса.
