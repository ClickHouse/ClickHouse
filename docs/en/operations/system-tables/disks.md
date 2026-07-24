---
description: 'System table containing information about disks defined in the server
  configuration'
keywords: ['system table', 'disks']
slug: /operations/system-tables/disks
title: 'system.disks'
---

import SystemTableCloud from '@site/docs/_snippets/_system_table_cloud.md';

<SystemTableCloud/>

Contains information about disks defined in the [server configuration](../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-multiple-volumes_configure).

Columns:

- `name` ([String](../../sql-reference/data-types/string.md)) — Name of a disk in the server configuration.
- `path` ([String](../../sql-reference/data-types/string.md)) — Path to the mount point in the file system.
- `free_space` ([UInt64](../../sql-reference/data-types/int-uint.md)) — Free space on disk in bytes.
- `total_space` ([UInt64](../../sql-reference/data-types/int-uint.md)) — Disk volume in bytes.
- `unreserved_space` ([UInt64](../../sql-reference/data-types/int-uint.md)) — Free space which is not taken by reservations (`free_space` minus the size of reservations taken by merges, inserts, and other disk write operations currently running).
- `keep_free_space` ([UInt64](../../sql-reference/data-types/int-uint.md)) — Amount of disk space that should stay free on disk in bytes. Defined in the `keep_free_space_bytes` parameter of disk configuration.

**Example**

```sql
SELECT * FROM system.disks;
```

```response
┌─name────┬─path─────────────────┬───free_space─┬──total_space─┬─keep_free_space─┐
│ default │ /var/lib/clickhouse/ │ 276392587264 │ 490652508160 │               0 │
└─────────┴──────────────────────┴──────────────┴──────────────┴─────────────────┘

1 rows in set. Elapsed: 0.001 sec.
```
