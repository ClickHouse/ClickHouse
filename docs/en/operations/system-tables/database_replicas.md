---
description: 'System table containing information about and status of replicated database.'
keywords: ['system table', 'database_replicas']
slug: /operations/system-tables/database_replicas
title: 'system.database_replicas'
doc_type: 'reference'
---

Contains information and status of all database replicas on current server.
Each database replica is represented by a single row.

Columns:

- `database` ([String](../../sql-reference/data-types/string.md)) — The name of the Replicated database is in.

- `is_readonly` ([UInt8](../../sql-reference/data-types/int-uint.md)) - Whether the database replica is in read-only mode.
    This mode is turned on if the config does not have sections with ClickHouse Keeper,
    if an unknown error occurred when reinitializing sessions in ClickHouse Keeper, and during session reinitialization in ClickHouse Keeper.

- `max_log_ptr` ([Int32](../../sql-reference/data-types/int-uint.md)) - Maximum entry number in the log of general activity.

- `replica_name` ([String](../../sql-reference/data-types/string.md)) - Replica name in ClickHouse Keeper. Different replicas of the same database have different names.

- `replica_path` ([String](../../sql-reference/data-types/string.md)) - Path to replica data in ClickHouse Keeper.

- `zookeeper_path` ([String](../../sql-reference/data-types/string.md)) - Path to database data in ClickHouse Keeper.

- `shard_name` ([String](../../sql-reference/data-types/string.md)) - Replica shard name.

- `log_ptr` ([Int32](../../sql-reference/data-types/int-uint.md)) - Maximum entry number in the log of general activity that the replica copied to its execution queue, plus one.
    If log_ptr is much smaller than max_log_ptr, something is wrong.

- `total_replicas` ([UInt32](../../sql-reference/data-types/int-uint.md)) - The total number of known replicas of this database.

- `zookeeper_exception` ([String](../../sql-reference/data-types/string.md)) - The last exception message, got if the error happened when fetching the info from ClickHouse Keeper.

- `is_session_expired` ([UInt8](../../sql-reference/data-types/int-uint.md)) - the session with ClickHouse Keeper has expired. Basically the same as `is_readonly`.

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