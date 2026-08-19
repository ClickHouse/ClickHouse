---
description: 'System table containing information about memory allocations done via
  jemalloc allocator in different size classes (bins) aggregated from all arenas.'
keywords: ['system table', 'jemalloc_bins']
slug: /operations/system-tables/jemalloc_bins
title: 'system.jemalloc_bins'
---

import SystemTableCloud from '@site/docs/_snippets/_system_table_cloud.md';

<SystemTableCloud/>

Contains information about memory allocations done via jemalloc allocator in different size classes (bins) aggregated from all arenas.
These statistics might not be absolutely accurate because of thread local caching in jemalloc.

Columns:

- `index` (UInt64) — Index of the bin ordered by size
- `large` (Bool) — True for large allocations and False for small
- `size` (UInt64) — Size of allocations in this bin
- `allocations` (UInt64) — Number of allocations
- `deallocations` (UInt64) — Number of deallocations

**Example**

Find the sizes of allocations that contributed the most to the current overall memory usage.

```sql
SELECT
    *,
    allocations - deallocations AS active_allocations,
    size * active_allocations AS allocated_bytes
FROM system.jemalloc_bins
WHERE allocated_bytes > 0
ORDER BY allocated_bytes DESC
LIMIT 10
```

```text
┌─index─┬─large─┬─────size─┬─allocactions─┬─deallocations─┬─active_allocations─┬─allocated_bytes─┐
│    82 │     1 │ 50331648 │            1 │             0 │                  1 │        50331648 │
│    10 │     0 │      192 │       512336 │        370710 │             141626 │        27192192 │
│    69 │     1 │  5242880 │            6 │             2 │                  4 │        20971520 │
│     3 │     0 │       48 │     16938224 │      16559484 │             378740 │        18179520 │
│    28 │     0 │     4096 │       122924 │        119142 │               3782 │        15491072 │
│    61 │     1 │  1310720 │        44569 │         44558 │                 11 │        14417920 │
│    39 │     1 │    28672 │         1285 │           913 │                372 │        10665984 │
│     4 │     0 │       64 │      2837225 │       2680568 │             156657 │        10026048 │
│     6 │     0 │       96 │      2617803 │       2531435 │              86368 │         8291328 │
│    36 │     1 │    16384 │        22431 │         21970 │                461 │         7553024 │
└───────┴───────┴──────────┴──────────────┴───────────────┴────────────────────┴─────────────────┘
```
