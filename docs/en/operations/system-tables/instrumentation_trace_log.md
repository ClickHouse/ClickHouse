---
description: 'System table containing the profiling log for instrumented functions'
keywords: ['system table', 'instrumentation_trace_log']
slug: /operations/system-tables/instrumentation_trace_log
title: 'system.instrumentation_trace_log'
doc_type: 'reference'
---

import SystemTableCloud from '@site/docs/_snippets/_system_table_cloud.md';

# system.instrumentation

<SystemTableCloud/>

Contains the profiling log for instrumentation points using LLVM's XRay feature.

Columns:
- `hostname` ([LowCardinality(String)](../../sql-reference/data-types/string.md)) — Hostname of the server executing the query.
- `event_date` ([Date](../../sql-reference/data-types/date.md)) — Event date.
- `event_time` ([DateTime](../../sql-reference/data-types/datetime.md)) — Event time.
- `event_time_microseconds` ([DateTime64](../../sql-reference/data-types/datetime64.md)) — Event time with microseconds resolution.
- `function_id` ([Int32](../../sql-reference/data-types/int-uint.md)) — ID assigned to the function in the `xray_instr_map` section of the ELF binary.
- `function_name` ([LowCardinality(String)](../../sql-reference/data-types/string.md)) — Name of the instrumented function.
- `tid` ([UInt64](../../sql-reference/data-types/int-uint.md)) — Thread ID.
- `duration_microseconds` ([UInt64](../../sql-reference/data-types/int-uint.md)) — Time the function was running for in microseconds.
- `query_id` ([String](../../sql-reference/data-types/string.md)) — ID of the query.
- `trace` ([Array(UInt64)](../../sql-reference/data-types/array.md)) — Stack trace at the moment of each function execution. Each element is a virtual memory address inside ClickHouse server process.

**Example**

```sql
SELECT * FROM system.instrumentation_trace_log;
```

```text
Row 1:
──────
hostname:                clickhouse.eu-central1.internal
event_date:              2025-11-06
event_time:              2025-11-06 11:40:18
event_time_microseconds: 1762429218266625 -- 1.76 quadrillion
function_id:             231168
function_name:           QueryMetricLog::startQuery
tid:                     319835
duration_microseconds:   36
query_id:                d8bce210-dba9-4721-8d06-5f47f265866c
trace:                   [451266699,452229447,452282529,452262642,512699474,512804196,614183943,614185445,613788543,613778001,128953254234819,128953254832320]

Row 2:
──────
hostname:                clickhouse.eu-central1.internal
event_date:              2025-11-06
event_time:              2025-11-06 11:40:23
event_time_microseconds: 1762429223237816 -- 1.76 quadrillion
function_id:             231168
function_name:           QueryMetricLog::startQuery
tid:                     319835
duration_microseconds:   18
query_id:                e8c7972c-8cfe-4d5f-9cba-6d9dc0bc6e00
trace:                   [451266699,452229447,452282529,452262642,512699474,512804196,614183943,614185445,613788543,613778001,128953254234819,128953254832320]

Row 3:
──────
hostname:                clickhouse.eu-central1.internal
event_date:              2025-11-06
event_time:              2025-11-06 11:40:23
event_time_microseconds: 1762429223833594 -- 1.76 quadrillion
function_id:             231168
function_name:           QueryMetricLog::startQuery
tid:                     319835
duration_microseconds:   17
query_id:                c0c0643c-5983-42de-a808-e0abe641cce6
trace:                   [451266699,452229447,452282529,452262642,512699474,512804196,614183943,614185445,613788543,613778001,128953254234819,128953254832320]

Row 4:
──────
hostname:                clickhouse.eu-central1.internal
event_date:              2025-11-06
event_time:              2025-11-06 11:46:25
event_time_microseconds: 1762429585177533 -- 1.76 quadrillion
function_id:             231168
function_name:           QueryMetricLog::startQuery
tid:                     330955
duration_microseconds:   11
query_id:                test_ayw15hq4_1_20104
trace:                   [451266699,452229447,452282529,452262642,512699474,512804196,614183943,614185445,613788543,613778001,128953254234819,128953254832320]
```

The profiling information can be converted easily to Chrome's Event Trace Format creating the following query in a `chrome_trace.sql` file:

```sql
SELECT
    format(
        '{{"traceEvents": [{}\n]}}',
        arrayStringConcat(
            groupArray(
                concat(
                    -- Begin Event
                    format(
                        '\n{{"name": "{}", "cat": "{}", "ph": "B", "ts": {}, "pid": 1, "tid": {}, "args": {{"query_id": "{}", "stack": "{}"}}}}',
                        function_name,
                        'clickhouse',
                        toString(event_time_microseconds),
                        toString(tid),
                        query_id,
                        concat('\n', arrayStringConcat(arrayMap((x, y) -> concat(x, ': ', y), arrayMap(x -> addressToLine(x), trace), arrayMap(x -> demangle(addressToSymbol(x)), trace))))
                    ),
                    ',',
                    -- End Event
                    format(
                        '\n{{"name": "{}", "cat": "{}", "ph": "E", "ts": {}, "pid": 1, "tid": {}, "args": {{"query_id": "{}", , "stack": "{}"}}}}',
                        function_name,
                        'clickhouse',
                        toString(event_time_microseconds + duration_microseconds),
                        toString(tid),
                        query_id,
                        concat('\n', arrayStringConcat(arrayMap((x, y) -> concat(x, ': ', y), arrayMap(x -> addressToLine(x), trace), arrayMap(x -> demangle(addressToSymbol(x)), trace))))
                    )
                )
            )
        )
    ) AS trace_json
FROM system.instrumentation_trace_log
WHERE event_date >= today()
SETTINGS allow_introspection_functions=1;
```

And executing it with ClickHouse Client to export it to a `trace.json` file that we can import either with [Perfetto](https://ui.perfetto.dev/) or [speedscope](https://www.speedscope.app/).

```bash
echo $(clickhouse client --query "$(cat chrome_trace.sql)") > trace.json
```

We can omit the stack part if we want a more compact but less informative trace.

**See also**

- [SYSTEM INSTRUMENT](../../sql-reference/statements/system.md) — Add or remove instrumentation points.
- [system.instrumentation](../../operations/system-tables/instrumentation.md) - Inspect instrumented functions.
