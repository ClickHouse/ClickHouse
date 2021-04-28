# system.trace_log {#system_tables-trace_log}

包含由采样查询分析器收集的堆栈跟踪。

设置 [trace_log](../../operations/server-configuration-parameters/settings.md#server_configuration_parameters-trace_log) 服务器配置部分后，ClickHouse会创建此表。另外，应该设置 [query_profiler_real_time_period_ns](../../operations/settings/settings.md#query_profiler_real_time_period_ns) 和 [query_profiler_cpu_time_period_ns](../../operations/settings/settings.md#query_profiler_cpu_time_period_ns) 。

要分析日志，请使用 `addressToLine`, `addressToSymbol` 和 `demangle` 内省函数。

列:

-   `event_date` ([Date](../../sql-reference/data-types/date.md)) — 采样时刻的日期。

-   `event_time` ([DateTime](../../sql-reference/data-types/datetime.md)) — 采样时刻的时间戳。

-   `timestamp_ns` ([UInt64](../../sql-reference/data-types/int-uint.md)) — 采样时刻的时间戳，以微秒为单位。

-   `revision` ([UInt32](../../sql-reference/data-types/int-uint.md)) — ClickHouse服务器内部版本。

    通过 `clickhouse-client` 方式连接到服务器时，你会看到类似于 `Connected to ClickHouse server version 19.18.1 revision 54429.` 的字符串. 该字段包含服务器的 `revision`，但包含 `version` 。

-   `trace_type` ([Enum8](../../sql-reference/data-types/enum.md)) — 跟踪类型：

    -   `Real` 表示按挂钟时间收集堆栈跟踪。
    -   `CPU` 表示按CPU时间收集堆栈跟踪。
    -   `Memory` 表示当内存分配超过后续水印时收集分配和释放。
    -   `MemorySample` 表示收集随机分配和解除分配。

-   `thread_number` ([UInt32](../../sql-reference/data-types/int-uint.md)) — 线程标识符。

-   `query_id` ([String](../../sql-reference/data-types/string.md)) — 查询标识符，可用于从 [query_log](#system_tables-query_log) 系统表中获取有关运行的查询的详细信息。

-   `trace` ([Array(UInt64)](../../sql-reference/data-types/array.md)) — 采样时的堆栈跟踪。每个元素都是ClickHouse服务器进程内部的虚拟内存地址。

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

 [原始文章](https://clickhouse.tech/docs/en/operations/system-tables/trace_log) <!--hide-->
 