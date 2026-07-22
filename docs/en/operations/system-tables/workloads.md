---
description: 'System table containing information for workloads residing on the local
  server.'
keywords: ['system table', 'workloads']
slug: /operations/system-tables/workloads
title: 'system.workloads'
---

# system.workloads

Contains information for [workloads](/operations/workload-scheduling.md#workload_entity_storage) residing on the local server. The table contains a row for every workload.

Example:

```sql
SELECT *
FROM system.workloads
FORMAT Vertical
```

```text
Row 1:
──────
name:         production
parent:       all
create_query: CREATE WORKLOAD production IN `all` SETTINGS weight = 9

Row 2:
──────
name:         development
parent:       all
create_query: CREATE WORKLOAD development IN `all`

Row 3:
──────
name:         all
parent:
create_query: CREATE WORKLOAD `all`
```

Columns:

- `name` (`String`) - Workload name.
- `parent` (`String`) - Parent workload name.
- `create_query` (`String`) - The definition of the workload.
