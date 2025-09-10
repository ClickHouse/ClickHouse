---
description: 'System table containing information about and status of replicated database.'
keywords: ['system table', 'database_replicas']
slug: /operations/system-tables/database_replicas
title: 'system.database_replicas'
---

Contains information of each Replicated database replicas.

Columns:

- `database` ([String](../../sql-reference/data-types/string.md)) — The name of the Replicated database is in.

- `is_readonly` ([UInt8](../../sql-reference/data-types/int-uint.md)) - Whether the database replica is in read-only mode.
    This mode is turned on if the config does not have sections with Zookeeper/ClickHouse Keeper.

- `is_session_expired` ([UInt8](../../sql-reference/data-types/int-uint.md)) - the session with ClickHouse Keeper has expired. Basically the same as `is_readonly`.

- `max_log_ptr` ([UInt32](../../sql-reference/data-types/int-uint.md)) - Maximum entry number in the log of general activity.

- `zookeeper_path` ([String](../../sql-reference/data-types/string.md)) - Path to database data in ClickHouse Keeper.

- `replica_name` ([String](../../sql-reference/data-types/string.md)) - Replica name in ClickHouse Keeper.

- `replica_path` ([String](../../sql-reference/data-types/string.md)) - Path to replica data in ClickHouse Keeper.

- `zookeeper_exception` ([String](../../sql-reference/data-types/string.md)) - The last exception message, got if the error happened when fetching the info from ClickHouse Keeper.

- `total_replicas` ([UInt32](../../sql-reference/data-types/int-uint.md)) - The total number of known replicas of this database.

- `log_ptr` ([UInt32](../../sql-reference/data-types/int-uint.md)) - Maximum entry number in the log of general activity that the replica copied to its execution queue, plus one.

**Example**

```sql
SELECT * FROM system.database_replicas FORMAT Vertical;
```

```text
Row 1:
──────
database:            db_2
is_readonly:         0
max_log_ptr:         2
replica_name:        replica1
replica_path:        /test/db_2/replicas/shard1|replica1
zookeeper_path:      /test/db_2
shard_name:          shard1
log_ptr:             2
total_replicas:      1
zookeeper_exception: 
is_session_expired:  0
```
