---
description: 'System table containing the instrumentation points'
keywords: ['system table', 'instrumentation']
slug: /operations/system-tables/instrumentation
title: 'system.instrumentation'
doc_type: 'reference'
---

import SystemTableCloud from '@site/docs/_snippets/_system_table_cloud.md';

# system.instrumentation

<SystemTableCloud/>

Contains the instrumentation points using LLVM's XRay feature.

Columns:
- `id` ([UInt32](../../sql-reference/data-types/int-uint.md)) — ID of the instrumentation point.
- `function_id` ([Int32](../../sql-reference/data-types/int-uint.md)) — ID assigned to the function in the `xray_instr_map` section of the ELF binary.
- `function_name` ([LowCardinality(String)](../../sql-reference/data-types/string.md)) — Name of the instrumented function.
- `handler` ([LowCardinality(String)](../../sql-reference/data-types/string.md)) — Handler type.
- `entry_type` ([LowCardinality(Nullable(String))](../../sql-reference/data-types/string.md)) — Entry type: Null, `ENTRY` or `EXIT`.
- `parameters` ([Dynamic](../../sql-reference/data-types/dynamic.md)) — Parameters for the handler call.

**Example**

```sql
SELECT * FROM system.instrumentation;
```

```text
   ┌─id─┬─function_id─┬─function_name──────────────┬─handler─┬─entry_type─┬─parameters─────┐
1. │  2 │      231414 │ QueryMetricLog::startQuery │ log     │ exit       │ this is a test │
2. │  1 │      231414 │ QueryMetricLog::startQuery │ profile │ ᴺᵁᴸᴸ       │ ᴺᵁᴸᴸ           │
3. │  0 │      231414 │ QueryMetricLog::startQuery │ sleep   │ entry      │ 0.5            │
   └────┴─────────────┴────────────────────────────┴─────────┴────────────┴────────────────┘
```

**See also**

- [SYSTEM INSTRUMENT](../../sql-reference/statements/system.md) — Add or remove instrumentation points.
- [system.instrumentation_trace_log](../../operations/system-tables/instrumentation_trace_log.md) - Inspect profiling log.
