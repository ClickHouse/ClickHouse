# system.replication_queue {#system_tables-replication_queue}

Contains information about tasks from replication queues stored in Zookeeper for tables in the `ReplicatedMergeTree` family.

Columns:

-   `database` ([String](../../sql-reference/data-types/string.md)) — Name of the database.

-   `table` ([String](../../sql-reference/data-types/string.md)) — Name of the table.

-   `replica_name` ([String](../../sql-reference/data-types/string.md)) — Replica name in ZooKeeper. Different replicas of the same table have different names.

-   `position` ([UInt32](../../sql-reference/data-types/int-uint.md)) — Position of task in the queue.

-   `node_name` ([String](../../sql-reference/data-types/string.md)) — Name of the queue.

-   `type` ([String](../../sql-reference/data-types/string.md)) — Type of task in the queue.

-   `create_time` ([Datetime](../../sql-reference/data-types/datetime.md)) — Date and time when the task was submitted for execution.

-   `required_quorum` ([UInt32](../../sql-reference/data-types/int-uint.md)) — Number of replicas waiting for the task to complete with confirmation of completion. This column is only relevant for the `GET PART` task.

-   `source_replica` ([String](../../sql-reference/data-types/string.md)) — Name of source replica.

-   `new_part_name` ([String](../../sql-reference/data-types/string.md)) — Name of new part.

-   `parts_to_merge` ([Array](../../sql-reference/data-types/array.md) ([String](../../sql-reference/data-types/string.md))) — Names of parts to merge or update.

-   `is_detach` ([UInt8](../../sql-reference/data-types/int-uint.md)) — Flag indicates whether the task is being performed using the `TO DETACHED` expression.

-   `is_currently_executing` ([UInt8](../../sql-reference/data-types/int-uint.md)) — Flag indicates whether a specific task is being performed right now.

-   `num_tries` ([UInt32](../../sql-reference/data-types/int-uint.md)) — Number of failed attempts to complete the task.

-   `last_exception` ([String](../../sql-reference/data-types/string.md)) — Text of the message about the last task execution error.

-   `last_attempt_time` ([Datetime](../../sql-reference/data-types/datetime.md)) — Date and time when the task was last attempted.

-   `num_postponed` ([UInt32](../../sql-reference/data-types/int-uint.md)) — Number of postponed tasks.

-   `postpone_reason` ([String](../../sql-reference/data-types/string.md)) — Reason why the task was postponed.

-   `last_postpone_time` ([Datetime](../../sql-reference/data-types/datetime.md)) — Date and time when the task was last postponed.

-   `merge_type` ([String](../../sql-reference/data-types/string.md)) — Type of current merge. Empty if it's an mutation.

**Example**

``` sql
SELECT * FROM system.replication_queue LIMIT 1 FORMAT Vertical;
```

``` text
Row 1:
──────
database:               merge
table:                  visits_v2
replica_name:           mtgiga001-1t.metrika.yandex.net
position:               15
node_name:              queue-0009325559
type:                   MERGE_PARTS
create_time:            2020-12-07 14:04:21
required_quorum:        0
source_replica:         mtgiga001-1t.metrika.yandex.net
new_part_name:          20201130_121373_121384_2
parts_to_merge:         ['20201130_121373_121378_1','20201130_121379_121379_0','20201130_121380_121380_0','20201130_121381_121381_0','20201130_121382_121382_0','20201130_121383_121383_0','20201130_121384_121384_0']
is_detach:              0
is_currently_executing: 0
num_tries:              36
last_exception:         Code: 226, e.displayText() = DB::Exception: Marks file '/opt/clickhouse/data/merge/visits_v2/tmp_fetch_20201130_121373_121384_2/CounterID.mrk' doesn't exist (version 20.8.7.15 (official build))
last_attempt_time:      2020-12-08 17:35:54
num_postponed:          0
postpone_reason:        
last_postpone_time:     1970-01-01 03:00:00
```

**See Also**

-   [Managing ReplicatedMergeTree Tables](../../sql-reference/statements/system.md/#query-language-system-replicated)

[Original article](https://clickhouse.tech/docs/en/operations/system_tables/replication_queue) <!--hide-->
