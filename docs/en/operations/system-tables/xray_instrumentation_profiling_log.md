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
- `name` ([String](../../sql-reference/data-types/string.md)) — Name of the instrumented function.
- `tid` ([UInt64](../../sql-reference/data-types/int-uint.md)) — Thread ID.
- `duration_microseconds` ([UInt64](../../sql-reference/data-types/int-uint.md)) — Time the function was running for in microseconds.
- `query_id` ([String](../../sql-reference/data-types/string.md)) — ID of the query.
- `function_id` ([Int32](../../sql-reference/data-types/int-uint.md)) — ID assigned to the function in the `xray_instr_map` section of the ELF binary.

**Example**

```sql
SELECT * FROM system.xray_instrumentation_profiling_log;
```

```text
    ┌─hostname────────────┬─event_date─┬──────────event_time─┬─event_time_microseconds─┬─name───────────────────────┬────tid─┬─duration_microseconds─┬─query_id─────────────────────────────┬─function_id─┐
 1. │ clickhouse.eu-central1.internal │ 2025-10-20 │ 2025-10-20 13:25:43 │        1760966743708573 │ QueryMetricLog::startQuery │ 185717 │                500131 │ 2d1d6a64-f590-4eea-9375-3ff523b25c1b │      224843 │
 2. │ clickhouse.eu-central1.internal │ 2025-10-20 │ 2025-10-20 13:25:47 │        1760966747074558 │ QueryMetricLog::startQuery │ 187788 │                500145 │ a70b1941-0d1f-42ed-ac7c-2fe4aa4b43ce │      224843 │
 3. │ clickhouse.eu-central1.internal │ 2025-10-20 │ 2025-10-20 13:25:47 │        1760966747076860 │ QueryMetricLog::startQuery │ 187790 │                500116 │ 4bec33b8-9ccd-40f1-8923-1972621b1f9a │      224843 │
 4. │ clickhouse.eu-central1.internal │ 2025-10-20 │ 2025-10-20 13:25:47 │        1760966747076878 │ QueryMetricLog::startQuery │ 187789 │                500113 │ 74dc2f41-41b2-41ec-8eef-8726ce304a9d │      224843 │
 5. │ clickhouse.eu-central1.internal │ 2025-10-20 │ 2025-10-20 13:25:47 │        1760966747083172 │ QueryMetricLog::startQuery │ 202383 │                500164 │ c3a1a676-5e1e-4e91-9d81-047618cc225f │      224843 │
 6. │ clickhouse.eu-central1.internal │ 2025-10-20 │ 2025-10-20 13:25:47 │        1760966747083279 │ QueryMetricLog::startQuery │ 202385 │                500129 │ f62be255-c8a3-4dfb-ad32-adde5f0a8327 │      224843 │
 7. │ clickhouse.eu-central1.internal │ 2025-10-20 │ 2025-10-20 13:25:47 │        1760966747083367 │ QueryMetricLog::startQuery │ 202384 │                500152 │ 1c7ae084-af92-4ed2-8050-8cc01591a384 │      224843 │
 8. │ clickhouse.eu-central1.internal │ 2025-10-20 │ 2025-10-20 13:25:47 │        1760966747086417 │ QueryMetricLog::startQuery │ 202393 │                500090 │ 80102752-ffd5-41f9-9f12-eb81edc53298 │      224843 │
 9. │ clickhouse.eu-central1.internal │ 2025-10-20 │ 2025-10-20 13:25:47 │        1760966747090897 │ QueryMetricLog::startQuery │ 202400 │                500116 │ 092a17a5-4a0a-4d3f-9d69-34d201159bd6 │      224843 │
10. │ clickhouse.eu-central1.internal │ 2025-10-20 │ 2025-10-20 13:25:47 │        1760966747091173 │ QueryMetricLog::startQuery │ 202399 │                500138 │ 58da1217-3f41-43c1-aa90-31a2fe91e75f │      224843 │
    └─────────────────────┴────────────┴─────────────────────┴─────────────────────────┴────────────────────────────┴────────┴───────────────────────┴──────────────────────────────────────┴─────────────┘
```

**See also**

- [SYSTEM INSTRUMENT](../../sql-reference/statements/system.md) — Add or remove instrumentation points.
- [system.xray_instrumentation](../../operations/system-tables/xray_instrumentation.md) - Inspect instrumented functions.
