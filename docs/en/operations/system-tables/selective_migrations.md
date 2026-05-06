---
description: 'System table showing active and completed partition migrations for tables with selective replication enabled.'
keywords: ['system table', 'selective_replication', 'migration']
sidebar_label: 'selective_migrations'
sidebar_position: 51
slug: /operations/system-tables/selective_migrations
title: 'system.selective_migrations'
doc_type: 'reference'
---

## Description {#description}

Shows active and completed partition migrations for tables with [selective replication](/engines/table-engines/mergetree-family/replication.md/#selective-replication) enabled (`replication_factor > 0`).

## Columns {#columns}

- `database` ([String](../../sql-reference/data-types/string.md)) — Database name.
- `table` ([String](../../sql-reference/data-types/string.md)) — Table name.
- `migration_id` ([String](../../sql-reference/data-types/string.md)) — UUID of the migration.
- `partition_id` ([String](../../sql-reference/data-types/string.md)) — Partition being migrated.
- `source_replica` ([String](../../sql-reference/data-types/string.md)) — Source replica name (the replica that currently stores the partition).
- `target_replica` ([String](../../sql-reference/data-types/string.md)) — Target replica name (the replica that will store the partition after migration).
- `state` ([String](../../sql-reference/data-types/string.md)) — Current state of the migration:
  - `CLONE` — Data is being copied from the source replica to the target replica.
  - `SWITCH` — The assignment is being atomically switched from source to target.
  - `CLEANUP` — Old data on the source replica is being cleaned up.
  - `DONE` — Migration completed successfully.
  - `FAILED` — Migration failed.
- `coordinator` ([String](../../sql-reference/data-types/string.md)) — Name of the replica coordinating the migration.
- `snapshot_parts` ([UInt64](../../sql-reference/data-types/int-uint.md)) — Number of parts in the source snapshot at the time of migration.
- `created_at` ([DateTime](../../sql-reference/data-types/datetime.md)) — Timestamp when the migration was created.

## Example {#example}

```sql
SELECT migration_id, partition_id, source_replica, target_replica, state
FROM system.selective_migrations
WHERE state NOT IN ('DONE', 'FAILED')
FORMAT Vertical;
```

## See Also {#see-also}

- [Selective Replication](/engines/table-engines/mergetree-family/replication.md/#selective-replication)
- [system.selective_assignments](/operations/system-tables/selective_assignments.md)
