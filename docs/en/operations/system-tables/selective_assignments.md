---
description: 'System table showing partition-to-replica assignments for tables with selective replication enabled.'
keywords: ['system table', 'selective_replication']
sidebar_label: 'selective_assignments'
sidebar_position: 51
slug: /operations/system-tables/selective_assignments
title: 'system.selective_assignments'
doc_type: 'reference'
---

## Description {#description}

Shows current partition-to-replica assignments for tables with [selective replication](/engines/table-engines/mergetree-family/replication.md/#selective-replication) enabled (`replication_factor > 0`).

## Columns {#columns}

- `database` ([String](../../sql-reference/data-types/string.md)) — Database name.
- `table` ([String](../../sql-reference/data-types/string.md)) — Table name.
- `partition_id` ([String](../../sql-reference/data-types/string.md)) — Partition identifier.
- `assigned_replicas` ([Array](../../sql-reference/data-types/array.md)([String](../../sql-reference/data-types/string.md))) — List of replica names assigned to store this partition.
- `replication_factor` ([UInt64](../../sql-reference/data-types/int-uint.md)) — Configured replication factor for the table.
- `is_local` ([UInt8](../../sql-reference/data-types/int-uint.md)) — Whether this partition is assigned to the current replica.
- `is_migrating` ([UInt8](../../sql-reference/data-types/int-uint.md)) — Whether this partition has an active migration (in `CLONE` or `SWITCH` state).

## Example {#example}

```sql
SELECT * FROM system.selective_assignments FORMAT Vertical;
```

## See Also {#see-also}

- [Selective Replication](/engines/table-engines/mergetree-family/replication.md/#selective-replication)
- [system.selective_migrations](/operations/system-tables/selective_migrations.md)
