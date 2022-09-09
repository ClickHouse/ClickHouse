# system.trace_log {#system_tables-trace_log}

Содержит экземпляры трассировки стека адресов вызова, собранные с помощью семплирующего профайлера запросов.

ClickHouse создает эту таблицу когда установлена настройка [trace_log](../server-configuration-parameters/settings.md#server_configuration_parameters-trace_log) в конфигурационном файле сервера. А также настройки [query_profiler_real_time_period_ns](../settings/settings.md#query_profiler_real_time_period_ns) и [query_profiler_cpu_time_period_ns](../settings/settings.md#query_profiler_cpu_time_period_ns).

Для анализа stack traces, используйте функции интроспекции `addressToLine`, `addressToSymbol` и `demangle`.

Столбцы:

-   `event_date`([Date](../../sql-reference/data-types/date.md)) — дата в момент снятия экземпляра стэка адресов вызова.

-   `event_time`([DateTime](../../sql-reference/data-types/datetime.md)) — дата и время в момент снятия экземпляра стэка адресов вызова.

-   `event_time_microseconds` ([DateTime64](../../sql-reference/data-types/datetime64.md)) — дата и время в момент снятия экземпляра стэка адресов вызова с точностью до микросекунд.

-   `revision`([UInt32](../../sql-reference/data-types/int-uint.md)) — ревизия сборки сервера ClickHouse.

        Во время соединения с сервером через `clickhouse-client`, вы видите строку похожую на `Connected to ClickHouse server version 19.18.1 revision 54429.`. Это поле содержит номер после `revision`, но не содержит строку после `version`.

-   `trace_type`([Enum8](../../sql-reference/data-types/enum.md)) — тип трассировки:

    -   `Real` — сбор трассировок стека адресов вызова по времени wall-clock.
    -   `CPU` — сбор трассировок стека адресов вызова по времени CPU.
    -   `Memory` — сбор выделенной памяти, когда ее размер превышает относительный инкремент.
    -   `MemorySample` — сбор случайно выделенной памяти.

-   `thread_number`([UInt32](../../sql-reference/data-types/int-uint.md)) — идентификатор треда.

-   `query_id`([String](../../sql-reference/data-types/string.md)) — идентификатор запроса который может быть использован для получения деталей о запросе из таблицы [query_log](query_log.md#system_tables-query_log) system table.

-   `trace`([Array(UInt64)](../../sql-reference/data-types/array.md)) — трассировка стека адресов вызова в момент семплирования. Каждый элемент массива — это адрес виртуальной памяти внутри процесса сервера ClickHouse.

**Пример**

``` sql
SELECT * FROM system.trace_log LIMIT 1 \G
```

``` text
Row 1:
──────
event_date:              2020-09-10
event_time:              2020-09-10 11:23:09
event_time_microseconds: 2020-09-10 11:23:09.872924
timestamp_ns:            1599762189872924510
revision:                54440
trace_type:              Memory
thread_id:               564963
query_id:
trace:                   [371912858,371912789,371798468,371799717,371801313,371790250,624462773,566365041,566440261,566445834,566460071,566459914,566459842,566459580,566459469,566459389,566459341,566455774,371993941,371988245,372158848,372187428,372187309,372187093,372185478,140222123165193,140222122205443]
size:                    5244400
```

