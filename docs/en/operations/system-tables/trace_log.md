# system.trace_log {#system_tables-trace_log}

Contains stack traces collected by the sampling query profiler.

ClickHouse creates this table when the [trace_log](../../operations/server-configuration-parameters/settings.md#server_configuration_parameters-trace_log) server configuration section is set. Also the [query_profiler_real_time_period_ns](../../operations/settings/settings.md#query_profiler_real_time_period_ns) and [query_profiler_cpu_time_period_ns](../../operations/settings/settings.md#query_profiler_cpu_time_period_ns) settings should be set.

To analyze logs, use the `addressToLine`, `addressToSymbol` and `demangle` introspection functions.

Columns:

-   `event_date` ([Date](../../sql-reference/data-types/date.md)) — Date of sampling moment.

-   `event_time` ([DateTime](../../sql-reference/data-types/datetime.md)) — Timestamp of the sampling moment.

-   `event_time_microseconds` ([DateTime64](../../sql-reference/data-types/datetime64.md)) — Timestamp of the sampling moment with microseconds precision.

-   `timestamp_ns` ([UInt64](../../sql-reference/data-types/int-uint.md)) — Timestamp of the sampling moment in nanoseconds.

-   `revision` ([UInt32](../../sql-reference/data-types/int-uint.md)) — ClickHouse server build revision.

    When connecting to the server by `clickhouse-client`, you see the string similar to `Connected to ClickHouse server version 19.18.1 revision 54429.`. This field contains the `revision`, but not the `version` of a server.

-   `trace_type` ([Enum8](../../sql-reference/data-types/enum.md)) — Trace type:

    -   `Real` represents collecting stack traces by wall-clock time.
    -   `CPU` represents collecting stack traces by CPU time.
    -   `Memory` represents collecting allocations and deallocations when memory allocation exceeds the subsequent watermark.
    -   `MemorySample` represents collecting random allocations and deallocations.

-   `thread_number` ([UInt32](../../sql-reference/data-types/int-uint.md)) — Thread identifier.

-   `query_id` ([String](../../sql-reference/data-types/string.md)) — Query identifier that can be used to get details about a query that was running from the [query_log](#system_tables-query_log) system table.

-   `trace` ([Array(UInt64)](../../sql-reference/data-types/array.md)) — Stack trace at the moment of sampling. Each element is a virtual memory address inside ClickHouse server process.

**Example**

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

 [Original article](https://clickhouse.tech/docs/en/operations/system-tables/trace_log) <!--hide-->
