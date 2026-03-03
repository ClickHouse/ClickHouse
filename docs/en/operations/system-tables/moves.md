---
description: 'System table containing information about in-progress data part moves
  of MergeTree tables. Each data part movement is represented by a single row.'
keywords: ['system table', 'moves']
slug: /operations/system-tables/moves
title: 'system.moves'
---

# system.moves

The table contains information about in-progress [data part moves](/sql-reference/statements/alter/partition#move-partitionpart) of [MergeTree](/engines/table-engines/mergetree-family/mergetree.md) tables. Each data part movement is represented by a single row.

Columns:

- `database` ([String](/sql-reference/data-types/string.md)) — Name of the database.

- `table` ([String](/sql-reference/data-types/string.md)) — Name of the table containing moving data part.

- `elapsed` ([Float64](../../sql-reference/data-types/float.md)) — Time elapsed (in seconds) since data part movement started.

- `target_disk_name` ([String](disks.md)) — Name of [disk](/operations/system-tables/disks/) to which the data part is moving.

- `target_disk_path` ([String](disks.md)) — Path to the mount point of the [disk](/operations/system-tables/disks/) in the file system.

- `part_name` ([String](/sql-reference/data-types/string.md)) — Name of the data part being moved.

- `part_size` ([UInt64](../../sql-reference/data-types/int-uint.md)) — Data part size.

- `thread_id` ([UInt64](../../sql-reference/data-types/int-uint.md)) — Identifier of a thread performing the movement.

**Example**

```sql
SELECT * FROM system.moves
```

```response
┌─database─┬─table─┬─────elapsed─┬─target_disk_name─┬─target_disk_path─┬─part_name─┬─part_size─┬─thread_id─┐
│ default  │ test2 │ 1.668056039 │ s3               │ ./disks/s3/      │ all_3_3_0 │       136 │    296146 │
└──────────┴───────┴─────────────┴──────────────────┴──────────────────┴───────────┴───────────┴───────────┘
```

**See Also**

- [MergeTree](/engines/table-engines/mergetree-family/mergetree.md) table engine
- [Using Multiple Block Devices for Data Storage](/engines/table-engines/mergetree-family/mergetree#table_engine-mergetree-multiple-volumes)
- [ALTER TABLE ... MOVE PART](/sql-reference/statements/alter/partition#move-partitionpart) command
