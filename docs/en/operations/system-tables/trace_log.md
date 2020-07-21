# system.trace\_log {#system_tables-trace_log}

Contains stack traces collected by the sampling query profiler.

ClickHouse creates this table when the [trace\_log](../../operations/server-configuration-parameters/settings.md#server_configuration_parameters-trace_log) server configuration section is set. Also the [query\_profiler\_real\_time\_period\_ns](../../operations/settings/settings.md#query_profiler_real_time_period_ns) and [query\_profiler\_cpu\_time\_period\_ns](../../operations/settings/settings.md#query_profiler_cpu_time_period_ns) settings should be set.

To analyze logs, use the `addressToLine`, `addressToSymbol` and `demangle` introspection functions.

Columns:

-   `event_date` ([Date](../../sql-reference/data-types/date.md)) — Date of sampling moment.

-   `event_time` ([DateTime](../../sql-reference/data-types/datetime.md)) — Timestamp of the sampling moment.

-   `timestamp_ns` ([UInt64](../../sql-reference/data-types/int-uint.md)) — Timestamp of the sampling moment in nanoseconds.

-   `revision` ([UInt32](../../sql-reference/data-types/int-uint.md)) — ClickHouse server build revision.

    When connecting to server by `clickhouse-client`, you see the string similar to `Connected to ClickHouse server version 19.18.1 revision 54429.`. This field contains the `revision`, but not the `version` of a server.

-   `timer_type` ([Enum8](../../sql-reference/data-types/enum.md)) — Timer type:

    -   `Real` represents wall-clock time.
    -   `CPU` represents CPU time.

-   `thread_number` ([UInt32](../../sql-reference/data-types/int-uint.md)) — Thread identifier.

-   `query_id` ([String](../../sql-reference/data-types/string.md)) — Query identifier that can be used to get details about a query that was running from the [query\_log](#system_tables-query_log) system table.

-   `trace` ([Array(UInt64)](../../sql-reference/data-types/array.md)) — Stack trace at the moment of sampling. Each element is a virtual memory address inside ClickHouse server process.

**Example**

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
