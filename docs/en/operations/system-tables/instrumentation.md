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
- `function_name` ([LowCardinality(String)](../../sql-reference/data-types/string.md)) — Name used to instrument the function.
- `handler` ([LowCardinality(String)](../../sql-reference/data-types/string.md)) — Handler type.
- `entry_type` ([Enum('Entry' = 0, 'Exit' = 1, 'EntryAndExit' = 2)](../../sql-reference/data-types/enum.md)) — Entry type: `Entry`, `Exit` or `EntryAndExit`.
- `symbol` ([LowCardinality(String)](../../sql-reference/data-types/string.md)) — Complete and demangled symbol.
- `parameters` ([Array(Dynamic)](../../sql-reference/data-types/array.md)) — Parameters for the handler call.

**Example**

```sql
SELECT * FROM system.instrumentation FORMAT Vertical;
```

```text
Row 1:
──────
id:            0
function_id:   231280
function_name: QueryMetricLog::startQuery
handler:       log
entry_type:    Entry
symbol:        DB::QueryMetricLog::startQuery(std::__1::basic_string<char, std::__1::char_traits<char>, std::__1::allocator<char>> const&, std::__1::chrono::time_point<std::__1::chrono::system_clock, std::__1::chrono::duration<long long, std::__1::ratio<1l, 1000000l>>>, unsigned long)
parameters:    ['test']

Row 2:
──────
id:            1
function_id:   231280
function_name: QueryMetricLog::startQuery
handler:       profile
entry_type:    EntryAndExit
symbol:        DB::QueryMetricLog::startQuery(std::__1::basic_string<char, std::__1::char_traits<char>, std::__1::allocator<char>> const&, std::__1::chrono::time_point<std::__1::chrono::system_clock, std::__1::chrono::duration<long long, std::__1::ratio<1l, 1000000l>>>, unsigned long)
parameters:    []

Row 3:
──────
id:            2
function_id:   231280
function_name: QueryMetricLog::startQuery
handler:       sleep
entry_type:    Exit
symbol:        DB::QueryMetricLog::startQuery(std::__1::basic_string<char, std::__1::char_traits<char>, std::__1::allocator<char>> const&, std::__1::chrono::time_point<std::__1::chrono::system_clock, std::__1::chrono::duration<long long, std::__1::ratio<1l, 1000000l>>>, unsigned long)
parameters:    [0.3]

3 rows in set. Elapsed: 0.302 sec.
```

**See also**

- [SYSTEM INSTRUMENT](../../sql-reference/statements/system.md#instrument) — Add or remove instrumentation points.
- [system.trace_log](../../operations/system-tables/trace_log.md) — Inspect profiling log.
- [system.symbols](../../operations/system-tables/symbols.md) — Inspect symbols to add instrumentation points.
