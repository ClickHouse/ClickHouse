---
description: 'System table containing information about ClickHouse server''s build options.'
slug: /operations/system-tables/build_options
title: 'system.build_options'
keywords: ['system table', 'build_options']
---

Contains information about the ClickHouse server's build options.

Columns:

- `name` (String) — Name of the build option, e.g. `USE_ODBC`
- `value` (String) — Value of the build option, e.g. `1`

**Example**

```sql
SELECT * FROM system.build_options LIMIT 5
```

```text
┌─name─────────────┬─value─┐
│ USE_BROTLI       │ 1     │
│ USE_BZIP2        │ 1     │
│ USE_CAPNP        │ 1     │
│ USE_CASSANDRA    │ 1     │
│ USE_DATASKETCHES │ 1     │
└──────────────────┴───────┘
```
