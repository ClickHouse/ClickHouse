---
slug: /en/operations/system-tables/replication_queue
---
# replication_queue

Contains information about tasks from replication queues stored in ClickHouse Keeper, or ZooKeeper, for tables in the `ReplicatedMergeTree` family.

Columns:

- `database` ([String](../../sql-reference/data-types/string.md)) — Name of the database.

- `table` ([String](../../sql-reference/data-types/string.md)) — Name of the table.

- `replica_name` ([String](../../sql-reference/data-types/string.md)) — Replica name in ClickHouse Keeper. Different replicas of the same table have different names.

- `position` ([UInt32](../../sql-reference/data-types/int-uint.md)) — Position of the task in the queue.

- `node_name` ([String](../../sql-reference/data-types/string.md)) — Node name in ClickHouse Keeper.

- `type` ([String](../../sql-reference/data-types/string.md)) — Type of the task in the queue, one of:

    - `GET_PART` — Get the part from another replica.
    - `ATTACH_PART` — Attach the part, possibly from our own replica (if found in the `detached` folder). You may think of it as a `GET_PART` with some optimizations as they're nearly identical.
    - `MERGE_PARTS` — Merge the parts.
    - `DROP_RANGE` — Delete the parts in the specified partition in the specified number range.
    - `CLEAR_COLUMN` — NOTE: Deprecated. Drop specific column from specified partition.
    - `CLEAR_INDEX` — NOTE: Deprecated. Drop specific index from specified partition.
    - `REPLACE_RANGE` — Drop a certain range of parts and replace them with new ones.
    - `MUTATE_PART` — Apply one or several mutations to the part.
    - `ALTER_METADATA` — Apply alter modification according to global /metadata and /columns paths.

- `create_time` ([DateTime](../../sql-reference/data-types/datetime.md)) — Date and time when the task was submitted for execution.

- `required_quorum` ([UInt32](../../sql-reference/data-types/int-uint.md)) — The number of replicas waiting for the task to complete with confirmation of completion. This column is only relevant for the `GET_PARTS` task.

- `source_replica` ([String](../../sql-reference/data-types/string.md)) — Name of the source replica.

- `new_part_name` ([String](../../sql-reference/data-types/string.md)) — Name of the new part.

- `parts_to_merge` ([Array](../../sql-reference/data-types/array.md) ([String](../../sql-reference/data-types/string.md))) — Names of parts to merge or update.

- `is_detach` ([UInt8](../../sql-reference/data-types/int-uint.md)) — The flag indicates whether the `DETACH_PARTS` task is in the queue.

- `is_currently_executing` ([UInt8](../../sql-reference/data-types/int-uint.md)) — The flag indicates whether a specific task is being performed right now.

- `num_tries` ([UInt32](../../sql-reference/data-types/int-uint.md)) — The number of failed attempts to complete the task.

- `last_exception` ([String](../../sql-reference/data-types/string.md)) — Text message about the last error that occurred (if any).

- `last_attempt_time` ([DateTime](../../sql-reference/data-types/datetime.md)) — Date and time when the task was last attempted.

- `num_postponed` ([UInt32](../../sql-reference/data-types/int-uint.md)) — The number of times the action was postponed.

- `postpone_reason` ([String](../../sql-reference/data-types/string.md)) — The reason why the task was postponed.

- `last_postpone_time` ([DateTime](../../sql-reference/data-types/datetime.md)) — Date and time when the task was last postponed.

- `merge_type` ([String](../../sql-reference/data-types/string.md)) — Type of the current merge. Empty if it's a mutation.

**Example**

``` sql
SELECT * FROM system.replication_queue LIMIT 1 FORMAT Vertical;
```

``` text
Row 1:
──────
database:               merge
table:                  visits_v2
replica_name:           mtgiga001-1t
position:               15
node_name:              queue-0009325559
type:                   MERGE_PARTS
create_time:            2020-12-07 14:04:21
required_quorum:        0
source_replica:         mtgiga001-1t
new_part_name:          20201130_121373_121384_2
parts_to_merge:         ['20201130_121373_121378_1','20201130_121379_121379_0','20201130_121380_121380_0','20201130_121381_121381_0','20201130_121382_121382_0','20201130_121383_121383_0','20201130_121384_121384_0']
is_detach:              0
is_currently_executing: 0
num_tries:              36
last_exception:         Code: 226, e.displayText() = DB::Exception: Marks file '/opt/clickhouse/data/merge/visits_v2/tmp_fetch_20201130_121373_121384_2/CounterID.mrk' does not exist (version 20.8.7.15 (official build))
last_attempt_time:      2020-12-08 17:35:54
num_postponed:          0
postpone_reason:
last_postpone_time:     1970-01-01 03:00:00
```

**See Also**

- [Managing ReplicatedMergeTree Tables](../../sql-reference/statements/system.md#query-language-system-replicated)
