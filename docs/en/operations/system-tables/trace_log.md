---
slug: /en/operations/system-tables/trace_log
---
# trace_log

Contains stack traces collected by the sampling query profiler.

ClickHouse creates this table when the [trace_log](../../operations/server-configuration-parameters/settings.md#server_configuration_parameters-trace_log) server configuration section is set. Also see settings: [query_profiler_real_time_period_ns](../../operations/settings/settings.md#query_profiler_real_time_period_ns), [query_profiler_cpu_time_period_ns](../../operations/settings/settings.md#query_profiler_cpu_time_period_ns), [memory_profiler_step](../../operations/settings/settings.md#memory_profiler_step),
[memory_profiler_sample_probability](../../operations/settings/settings.md#memory_profiler_sample_probability), [trace_profile_events](../../operations/settings/settings.md#trace_profile_events).

To analyze logs, use the `addressToLine`, `addressToLineWithInlines`, `addressToSymbol` and `demangle` introspection functions.

Columns:

- `hostname` ([LowCardinality(String)](../../sql-reference/data-types/string.md)) — Hostname of the server executing the query.
- `event_date` ([Date](../../sql-reference/data-types/date.md)) — Date of sampling moment.
- `event_time` ([DateTime](../../sql-reference/data-types/datetime.md)) — Timestamp of the sampling moment.
- `event_time_microseconds` ([DateTime64](../../sql-reference/data-types/datetime64.md)) — Timestamp of the sampling moment with microseconds precision.
- `timestamp_ns` ([UInt64](../../sql-reference/data-types/int-uint.md)) — Timestamp of the sampling moment in nanoseconds.
- `revision` ([UInt32](../../sql-reference/data-types/int-uint.md)) — ClickHouse server build revision.

    When connecting to the server by `clickhouse-client`, you see the string similar to `Connected to ClickHouse server version 19.18.1.`. This field contains the `revision`, but not the `version` of a server.

- `trace_type` ([Enum8](../../sql-reference/data-types/enum.md)) — Trace type:
    - `Real` represents collecting stack traces by wall-clock time.
    - `CPU` represents collecting stack traces by CPU time.
    - `Memory` represents collecting allocations and deallocations when memory allocation exceeds the subsequent watermark.
    - `MemoryPeak` represents collecting updates of peak memory usage.
    - `MemorySample` represents collecting random allocations and deallocations.
    - `MemoryProfile` represents a periodic dump of a random sample of active allocations, see [heap_profiler_log_sample_rate](../../operations/server-configuration-parameters/settings.md#heap_profiler_log_sample_rate).
    - `ProfileEvent` represents collecting of increments of profile events.
- `thread_id` ([UInt64](../../sql-reference/data-types/int-uint.md)) — Thread identifier.
- `query_id` ([String](../../sql-reference/data-types/string.md)) — Query identifier that can be used to get details about a query that was running from the [query_log](#system_tables-query_log) system table.
- `trace` ([Array(UInt64)](../../sql-reference/data-types/array.md)) — Stack trace at the moment of sampling. Each element is a virtual memory address inside ClickHouse server process.
- `size` ([Int64](../../sql-reference/data-types/int-uint.md)) - For trace types `Memory`, `MemorySample` or `MemoryPeak` is the amount of memory allocated, for other trace types is 0.
- `event` ([LowCardinality(String)](../../sql-reference/data-types/lowcardinality.md)) - For trace type `ProfileEvent` is the name of updated profile event, for other trace types is an empty string.
- `increment` ([UInt64](../../sql-reference/data-types/int-uint.md)) - For trace type `ProfileEvent` is the amount of increment of profile event, for other trace types is 0.
- `weight` ([Int64](../../sql-reference/data-types/int-uint.md)) - For trace type `MemoryProfile`: the amount of memory that this row represents, in bytes. Different from `size`, which is the size of the allocation. For things like flame graphs, it's important to use `weight` and not `size`. E.g. a sample randomly chosen from 1000 8-byte allocations would have weight 8000, same as a sample representing one 8000-byte allocation. For other trace types: 1.
- `profile_id` ([LowCardinality(String)](../../sql-reference/data-types/string.md)) - For trace type `MemoryProfile`: UUID of the dump (snapshot). All rows with the same `profile_id` are written simultaneously, atomically. Normally you would make a flame graph out of one such dump, or the union of dumps from multiple servers.

**Example**

``` sql
SELECT * FROM system.trace_log LIMIT 1 \G
```

``` text
Row 1:
──────
hostname:                clickhouse.eu-central1.internal
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
