---
description: 'System table containing the profiling log for instrumented functions'
keywords: ['system table', 'xray_instrumentation_profiling_log']
slug: /operations/system-tables/xray_instrumentation_profiling_log
title: 'system.xray_instrumentation_profiling_log'
doc_type: 'reference'
---

import SystemTableCloud from '@site/docs/_snippets/_system_table_cloud.md';

# system.xray_instrumentation

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

**Example**

```sql
SELECT * FROM system.xray_instrumentation_profiling_log;
```

```text
   ┌─hostname────────────┬─event_date─┬──────────event_time─┬─event_time_microseconds─┬─function_id─┬─function_name──────────────┬────tid─┬─duration_microseconds─┬─query_id────────────────────────────
─┐
1. │ clickhouse.eu-central1.internal │ 2025-10-28 │ 2025-10-28 10:43:33 │        1761648213503121 │      231414 │ QueryMetricLog::startQuery │ 165136 │                    13 │ 307fddf4-9cd8-410e-bd1d-c19234cd25a5
 │
2. │ clickhouse.eu-central1.internal │ 2025-10-28 │ 2025-10-28 10:43:33 │        1761648213601136 │      231414 │ QueryMetricLog::startQuery │ 165136 │                    23 │ test_a0nqowt1_profile
 │
3. │ clickhouse.eu-central1.internal │ 2025-10-28 │ 2025-10-28 10:45:06 │        1761648306766602 │      231414 │ QueryMetricLog::startQuery │ 165136 │                    14 │ 90ba8906-5c5b-4024-98a1-b58cf804ed62
 │
4. │ clickhouse.eu-central1.internal │ 2025-10-28 │ 2025-10-28 10:45:06 │        1761648306864515 │      231414 │ QueryMetricLog::startQuery │ 165136 │                    21 │ test_bwgh9lfl_profile
 │
   └─────────────────────┴────────────┴─────────────────────┴─────────────────────────┴─────────────┴────────────────────────────┴────────┴───────────────────────┴─────────────────────────────────────
 ┘
```

The profiling information can be converted easily to Chrome's Event Trace Format creating the following query in a `chrome_trace.sql` file:

```sql
SELECT
    format(
        '{{"traceEvents": [{}]}}',
        arrayStringConcat(
            groupArray(
                concat(
                    -- Begin Event
                    format(
                        '\n{{"name": "{}", "cat": "{}", "ph": "B", "ts": {}, "pid": 1, "tid": {}, "args": {{"query_id": "{}"}}}}',
                        name,
                        'clickhouse',
                        toString(event_time_microseconds),
                        toString(tid),
                        query_id
                    ),
                    ',',
                    -- End Event
                    format(
                        '\n{{"name": "{}", "cat": "{}", "ph": "E", "ts": {}, "pid": 1, "tid": {}, "args": {{"query_id": "{}"}}}}',
                        name,
                        'clickhouse',
                        toString(event_time_microseconds + duration_microseconds),
                        toString(tid),
                        query_id
                    )
                )
            ),
            ','
        )
    ) AS trace_json
FROM system.xray_instrumentation_profiling_log
WHERE event_date >= today();
```

And executing it with ClickHouse Client to export it to a `trace.json` file that we can import either with [Perfetto](https://ui.perfetto.dev/) or [speedscope](https://www.speedscope.app/).

```bash
clickhouse client --query "$(cat chrome_trace.sql)" > trace.json
```

**See also**

- [SYSTEM INSTRUMENT](../../sql-reference/statements/system.md) — Add or remove instrumentation points.
- [system.xray_instrumentation](../../operations/system-tables/xray_instrumentation.md) - Inspect instrumented functions.
