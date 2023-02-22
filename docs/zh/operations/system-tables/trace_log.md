---
machine_translated: true
machine_translated_rev: 5decc73b5dc60054f19087d3690c4eb99446a6c3
---

# 系统。trace_log {#system_tables-trace_log}

包含采样查询探查器收集的堆栈跟踪。

ClickHouse创建此表时 [trace_log](../../operations/server-configuration-parameters/settings.md#server_configuration_parameters-trace_log) 服务器配置部分被设置。 也是 [query_profiler_real_time_period_ns](../../operations/settings/settings.md#query_profiler_real_time_period_ns) 和 [query_profiler_cpu_time_period_ns](../../operations/settings/settings.md#query_profiler_cpu_time_period_ns) 应设置设置。

要分析日志，请使用 `addressToLine`, `addressToSymbol` 和 `demangle` 内省功能。

列:

-   `event_date` ([日期](../../sql-reference/data-types/date.md)) — Date of sampling moment.

-   `event_time` ([日期时间](../../sql-reference/data-types/datetime.md)) — Timestamp of the sampling moment.

-   `timestamp_ns` ([UInt64](../../sql-reference/data-types/int-uint.md)) — Timestamp of the sampling moment in nanoseconds.

-   `revision` ([UInt32](../../sql-reference/data-types/int-uint.md)) — ClickHouse server build revision.

    通过以下方式连接到服务器 `clickhouse-client`，你看到的字符串类似于 `Connected to ClickHouse server version 19.18.1 revision 54429.`. 该字段包含 `revision`，但不是 `version` 的服务器。

-   `timer_type` ([枚举8](../../sql-reference/data-types/enum.md)) — Timer type:

    -   `Real` 表示挂钟时间。
    -   `CPU` 表示CPU时间。

-   `thread_number` ([UInt32](../../sql-reference/data-types/int-uint.md)) — Thread identifier.

-   `query_id` ([字符串](../../sql-reference/data-types/string.md)) — Query identifier that can be used to get details about a query that was running from the [query_log](#system_tables-query_log) 系统表.

-   `trace` ([数组(UInt64)](../../sql-reference/data-types/array.md)) — Stack trace at the moment of sampling. Each element is a virtual memory address inside ClickHouse server process.

**示例**

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
