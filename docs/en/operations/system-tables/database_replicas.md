---
description: 'System table containing information about and status of replicated database.'
keywords: ['system table', 'database_replicas']
slug: /operations/system-tables/database_replicas
title: 'system.database_replicas'
---

Contains information of each detached table.

Columns:

- `database` ([String](../../sql-reference/data-types/string.md)) — The name of the database the table is in.

- `is_readonly` ([UInt8](../../sql-reference/data-types/int-uint.md)) - Whether the database replica is in read-only mode.
    This mode is turned on if the config does not have sections with Zookeeper/ClickHouse Keeper.

- `max_log_ptr` ([UInt32](../../sql-reference/data-types/int-uint.md)) - Maximum entry number in the log of general activity.


**Example**

```sql
SELECT * FROM system.database_replicas FORMAT Vertical;
```

```text
Row 1:
──────
database:                   base
is_readonly:                1
max_log_ptr:                16
```
