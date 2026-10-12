---
description: 'System table containing stack traces collected by the sampling query
  profiler.'
keywords: ['system table', 'trace_log']
slug: /operations/system-tables/trace_log
title: 'system.trace_log'
doc_type: 'reference'
---

import SystemTableCloud from '@site/docs/_snippets/_system_table_cloud.md';

# system.trace_log

<SystemTableCloud/>

Contains stack traces collected by the [sampling query profiler](../../operations/optimizing-performance/sampling-query-profiler.md).

ClickHouse creates this table when the [trace_log](../../operations/server-configuration-parameters/settings.md#trace_log) server configuration section is set. Also see settings: [query_profiler_real_time_period_ns](../../operations/settings/settings.md#query_profiler_real_time_period_ns), [query_profiler_cpu_time_period_ns](../../operations/settings/settings.md#query_profiler_cpu_time_period_ns), [memory_profiler_step](../../operations/settings/settings.md#memory_profiler_step),
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
  - `MemorySample` represents collecting random allocations and deallocations.
  - `MemoryPeak` represents collecting updates of peak memory usage.
  - `ProfileEvent` represents collecting of increments of profile events.
  - `JemallocSample` represents collecting of jemalloc samples.
  - `MemoryAllocatedWithoutCheck` represents collection of significant allocations (>16MiB) that is done with ignoring any memory limits (for ClickHouse developers only).
  - `Instrumentation` represents traces collected by the instrumentation performed through XRay.
- `cpu_id` ([UInt64](../../sql-reference/data-types/int-uint.md)) — CPU identifier.
- `thread_id` ([UInt64](../../sql-reference/data-types/int-uint.md)) — Thread identifier.
- `query_id` ([String](../../sql-reference/data-types/string.md)) — Query identifier that can be used to get details about a query that was running from the [query_log](/operations/system-tables/query_log) system table.
- `trace` ([Array(UInt64)](../../sql-reference/data-types/array.md)) — Stack trace at the moment of sampling. Each element is a virtual memory address inside ClickHouse server process.
- `size` ([Int64](../../sql-reference/data-types/int-uint.md)) - For trace types `Memory`, `MemorySample` or `MemoryPeak` is the amount of memory allocated, for other trace types is 0.
- `event` ([LowCardinality(String)](../../sql-reference/data-types/lowcardinality.md)) - For trace type `ProfileEvent` is the name of updated profile event, for other trace types is an empty string.
- `increment` ([UInt64](../../sql-reference/data-types/int-uint.md)) - For trace type `ProfileEvent` is the amount of increment of profile event, for other trace types is 0.
- `symbols`, ([Array(LowCardinality(String))](../../sql-reference/data-types/array.md)), If the symbolization is enabled, contains demangled symbol names, corresponding to the `trace`.
- `lines`, ([Array(LowCardinality(String))](../../sql-reference/data-types/array.md)), If the symbolization is enabled, contains strings with file names with line numbers, corresponding to the `trace`.
- `function_id` ([Nullable(Int32)](../../sql-reference/data-types/nullable.md)), For trace type Instrumentation, ID assigned to the function in xray_instr_map section of elf-binary.
- `function_name` ([Nullable(String)](../../sql-reference/data-types/nullable.md)), For trace type Instrumentation, name of the instrumented function.
- `handler` ([Nullable(String)](../../sql-reference/data-types/nullable.md)), For trace type Instrumentation, handler of the instrumented function.
- `entry_type` ([Nullable(Enum('Entry' = 0, 'Exit' = 1))](../../sql-reference/data-types/nullable.md)), For trace type Instrumentation, entry type of the trace.
- `duration_nanoseconds` ([Nullable(UInt64)](../../sql-reference/data-types/nullable.md)), For trace type Instrumentation, time the function was running for in nanoseconds.

The symbolization can be enabled or disabled in the `symbolize` under `trace_log` in the server's configuration file.

**Example**

```sql
SELECT * FROM system.trace_log LIMIT 1 \G
```

```text
Row 1:
──────
hostname:                clickhouse.eu-central1.internal
event_date:              2025-11-11
event_time:              2025-11-11 11:53:59
event_time_microseconds: 2025-11-11 11:53:59.128333
timestamp_ns:            1762862039128333000
revision:                54504
trace_type:              Instrumentation
cpu_id:                  19
thread_id:               3166432 -- 3.17 million
query_id:                ef462508-e189-4ea2-b231-4489506728e8
trace:                   [350594916,447733712,447742095,447727324,447726659,221642873,450882315,451852359,451905441,451885554,512404306,512509092,612861767,612863269,612466367,612455825,137631896259267,137631896856768]
size:                    0
ptr:                     0
memory_context:          Unknown
memory_blocked_context:  Unknown
event:
increment:               0
symbols:                 ['StackTrace::StackTrace()','DB::InstrumentationManager::createTraceLogElement(DB::InstrumentationManager::InstrumentedPointInfo const&, XRayEntryType, std::__1::chrono::time_point<std::__1::chrono::system_clock, std::__1::chrono::duration<long long, std::__1::ratio<1l, 1000000l>>>) const','DB::InstrumentationManager::profile(XRayEntryType, DB::InstrumentationManager::InstrumentedPointInfo const&)','DB::InstrumentationManager::dispatchHandlerImpl(int, XRayEntryType)','DB::InstrumentationManager::dispatchHandler(int, XRayEntryType)','__xray_FunctionEntry','DB::QueryMetricLog::startQuery(std::__1::basic_string<char, std::__1::char_traits<char>, std::__1::allocator<char>> const&, std::__1::chrono::time_point<std::__1::chrono::system_clock, std::__1::chrono::duration<long long, std::__1::ratio<1l, 1000000l>>>, unsigned long)','DB::logQueryStart(std::__1::chrono::time_point<std::__1::chrono::system_clock, std::__1::chrono::duration<long long, std::__1::ratio<1l, 1000000l>>> const&, std::__1::shared_ptr<DB::Context> const&, std::__1::basic_string<char, std::__1::char_traits<char>, std::__1::allocator<char>> const&, unsigned long, std::__1::shared_ptr<DB::IAST> const&, DB::QueryPipeline const&, DB::IInterpreter const*, bool, std::__1::basic_string<char, std::__1::char_traits<char>, std::__1::allocator<char>> const&, std::__1::basic_string<char, std::__1::char_traits<char>, std::__1::allocator<char>> const&, bool)','DB::executeQueryImpl(char const*, char const*, std::__1::shared_ptr<DB::Context>, DB::QueryFlags, DB::QueryProcessingStage::Enum, std::__1::unique_ptr<DB::ReadBuffer, std::__1::default_delete<DB::ReadBuffer>>&, std::__1::shared_ptr<DB::IAST>&, std::__1::shared_ptr<DB::ImplicitTransactionControlExecutor>, std::__1::function<void ()>)','DB::executeQuery(std::__1::basic_string<char, std::__1::char_traits<char>, std::__1::allocator<char>> const&, std::__1::shared_ptr<DB::Context>, DB::QueryFlags, DB::QueryProcessingStage::Enum)','DB::TCPHandler::runImpl()','DB::TCPHandler::run()','Poco::Net::TCPServerConnection::start()','Poco::Net::TCPServerDispatcher::run()','Poco::PooledThread::run()','Poco::ThreadImpl::runnableEntry(void*)','start_thread','__clone3']
lines:                   ['./build/../src/Common/StackTrace.cpp:395','./src/Common/StackTrace.h:62','./contrib/llvm-project/libcxx/include/__memory/shared_ptr.h:738','./build/./src/Interpreters/InstrumentationManager.cpp:257','./build/./src/Interpreters/InstrumentationManager.cpp:225','/home/ubuntu/ClickHouse/ClickHouse/build/programs/clickhouse','./build/./src/Interpreters/QueryMetricLog.cpp:0','./contrib/llvm-project/libcxx/include/__memory/shared_ptr.h:667','./build/./src/Interpreters/executeQuery.cpp:0','./build/./src/Interpreters/executeQuery.cpp:0','./contrib/llvm-project/libcxx/include/__memory/shared_ptr.h:744','./contrib/llvm-project/libcxx/include/__memory/shared_ptr.h:583','./build/../base/poco/Net/src/TCPServerConnection.cpp:54','../contrib/llvm-project/libcxx/include/__memory/unique_ptr.h:80','./build/../base/poco/Foundation/src/ThreadPool.cpp:219','../base/poco/Foundation/include/Poco/AutoPtr.h:77','/home/ubuntu/ClickHouse/ClickHouse/build/programs/clickhouse','/home/ubuntu/ClickHouse/ClickHouse/build/programs/clickhouse']
function_id:             231255
function_name:           DB::QueryMetricLog::startQuery(std::__1::basic_string<char, std::__1::char_traits<char>, std::__1::allocator<char>> const&, std::__1::chrono::time_point<std::__1::chrono::system_clock, std::__1::chrono::duration<long long, std::__1::ratio<1l, 1000000l>>>, unsigned long)
handler:                 profile
entry_type:              Exit
duration_nanoseconds:   58435
```

## Converting to Chrome Event Trace Format {#chrome-event-trace-format}

The profiling data can be converted to Chrome's Event Trace Format with the following query. Save the query to a `chrome_trace.sql` file:

```sql
WITH traces AS (
    SELECT * FROM system.trace_log
    WHERE event_date >= today() AND trace_type = 'Instrumentation' AND handler = 'profile'
    ORDER BY event_time, entry_type
)
SELECT
    format(
        '{{"traceEvents": [{}\n]}}',
        arrayStringConcat(
            groupArray(
                format(
                    '\n{{"name": "{}", "cat": "clickhouse", "ph": "{}", "ts": {}, "pid": 1, "tid": {}, "args": {{"query_id": "{}", "cpu_id": {}, "stack": [{}]}}}},',
                    function_name,
                    if(entry_type = 0, 'B', 'E'),
                    timestamp_ns/1000,
                    toString(thread_id),
                    query_id,
                    cpu_id,
                    arrayStringConcat(arrayMap((x, y) -> concat('"', x, ': ', y, '", '), lines, symbols))
                )
            )
        )
    )
FROM traces;
```

And executing it with ClickHouse Client to export it to a `trace.json` file that we can import either with [Perfetto](https://ui.perfetto.dev/) or [speedscope](https://www.speedscope.app/).

```bash
echo $(clickhouse client --query "$(cat chrome_trace.sql)") > trace.json
```

We can omit the stack part if we want a more compact but less informative trace.

**See also**

- [SYSTEM INSTRUMENT](../../sql-reference/statements/system.md#instrument) — Add or remove instrumentation points.
- [system.instrumentation](../../operations/system-tables/instrumentation.md) — Inspect instrumented points.
- [system.symbols](../../operations/system-tables/symbols.md) — Inspect symbols to add instrumentation points.
