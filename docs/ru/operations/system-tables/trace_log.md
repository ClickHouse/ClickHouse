# system.trace_log {#system_tables-trace_log}

Содержит экземпляры трассировки стека адресов вызова, собранные с помощью семплирующего профайлера запросов.

ClickHouse создает эту таблицу когда утсановлена настройка [trace\_log](../server-configuration-parameters/settings.md#server_configuration_parameters-trace_log) в конфигурационном файле сервереа. А также настройки [query_profiler_real_time_period_ns](../settings/settings.md#query_profiler_real_time_period_ns) и [query_profiler_cpu_time_period_ns](../settings/settings.md#query_profiler_cpu_time_period_ns).

Для анализа stack traces, используйте функции интроспекции `addressToLine`, `addressToSymbol` и `demangle`.

Колонки:

-   `event_date`([Date](../../sql-reference/data-types/date.md)) — Дата в момент снятия экземпляра стэка адресов вызова.

-   `event_time`([DateTime](../../sql-reference/data-types/datetime.md)) — Дата и время в момент снятия экземпляра стэка адресов вызова.

-   `revision`([UInt32](../../sql-reference/data-types/int-uint.md)) — ревизия сборки сервера ClickHouse.

        Во время соединения с сервером через `clickhouse-client`, вы видите строку похожую на `Connected to ClickHouse server version 19.18.1 revision 54429.`. Это поле содержит номер после `revision`, но не содержит строку после `version`.

-   `timer_type`([Enum8](../../sql-reference/data-types/enum.md)) — Тип таймера:

        - `Real` означает wall-clock время.
        - `CPU` означает относительное CPU время.

-   `thread_number`([UInt32](../../sql-reference/data-types/int-uint.md)) — Идентификатор треда.

-   `query_id`([String](../../sql-reference/data-types/string.md)) — Идентификатор запроса который может быть использован для получения деталей о запросе из таблицы [query_log](query_log.md#system_tables-query_log) system table.

-   `trace`([Array(UInt64)](../../sql-reference/data-types/array.md)) — Трассировка стека адресов вызова в момент семплирования. Каждый элемент массива это адрес виртуальной памяти внутри процесса сервера ClickHouse.

**Пример**

``` sql
SELECT * FROM system.trace_log LIMIT 1 \G
```

``` text
Row 1:
──────
event_date:    2019-11-15
event_time:    2019-11-15 15:09:38
revision:      54428
timer_type:    Real
thread_number: 48
query_id:      acc4d61f-5bd1-4a3e-bc91-2180be37c915
trace:         [94222141367858,94222152240175,94222152325351,94222152329944,94222152330796,94222151449980,94222144088167,94222151682763,94222144088167,94222151682763,94222144088167,94222144058283,94222144059248,94222091840750,94222091842302,94222091831228,94222189631488,140509950166747,140509942945935]
```

[Оригинальная статья](https://clickhouse.tech/docs/ru/operations/system_tables/trace_log) <!--hide-->
