# replicas {#system_tables-replicas}

Contains information and status for replicated tables residing on the local server.
This table can be used for monitoring. The table contains a row for every Replicated\* table.

Example:

``` sql
SELECT *
FROM system.replicas
WHERE table = 'test_table'
FORMAT Vertical
```

``` text
Query id: dc6dcbcb-dc28-4df9-ae27-4354f5b3b13e

Row 1:
───────
database:                    db
table:                       test_table
engine:                      ReplicatedMergeTree
is_leader:                   1
can_become_leader:           1
is_readonly:                 0
is_session_expired:          0
future_parts:                0
parts_to_check:              0
zookeeper_path:              /test/test_table
replica_name:                r1
replica_path:                /test/test_table/replicas/r1
columns_version:             -1
queue_size:                  27
inserts_in_queue:            27
merges_in_queue:             0
part_mutations_in_queue:     0
queue_oldest_time:           2021-10-12 14:48:48
inserts_oldest_time:         2021-10-12 14:48:48
merges_oldest_time:          1970-01-01 03:00:00
part_mutations_oldest_time:  1970-01-01 03:00:00
oldest_part_to_get:          1_17_17_0
oldest_part_to_merge_to:     
oldest_part_to_mutate_to:    
log_max_index:               206
log_pointer:                 207
last_queue_update:           2021-10-12 14:50:08
absolute_delay:              99
total_replicas:              5
active_replicas:             5
last_queue_update_exception: 
zookeeper_exception:         
replica_is_active:           {'r1':1,'r2':1}
```

Columns:

-   `database` (`String`) - Database name
-   `table` (`String`) - Table name
-   `engine` (`String`) - Table engine name
-   `is_leader` (`UInt8`) - Whether the replica is the leader.
    Multiple replicas can be leaders at the same time. A replica can be prevented from becoming a leader using the `merge_tree` setting `replicated_can_become_leader`. The leaders are responsible for scheduling background merges.
    Note that writes can be performed to any replica that is available and has a session in ZK, regardless of whether it is a leader.
-   `can_become_leader` (`UInt8`) - Whether the replica can be a leader.
-   `is_readonly` (`UInt8`) - Whether the replica is in read-only mode.
    This mode is turned on if the config does not have sections with ZooKeeper, if an unknown error occurred when reinitializing sessions in ZooKeeper, and during session reinitialization in ZooKeeper.
-   `is_session_expired` (`UInt8`) - the session with ZooKeeper has expired. Basically the same as `is_readonly`.
-   `future_parts` (`UInt32`) - The number of data parts that will appear as the result of INSERTs or merges that haven’t been done yet.
-   `parts_to_check` (`UInt32`) - The number of data parts in the queue for verification. A part is put in the verification queue if there is suspicion that it might be damaged.
-   `zookeeper_path` (`String`) - Path to table data in ZooKeeper.
-   `replica_name` (`String`) - Replica name in ZooKeeper. Different replicas of the same table have different names.
-   `replica_path` (`String`) - Path to replica data in ZooKeeper. The same as concatenating ‘zookeeper_path/replicas/replica_path’.
-   `columns_version` (`Int32`) - Version number of the table structure. Indicates how many times ALTER was performed. If replicas have different versions, it means some replicas haven’t made all of the ALTERs yet.
-   `queue_size` (`UInt32`) - Size of the queue for operations waiting to be performed. Operations include inserting blocks of data, merges, and certain other actions. It usually coincides with `future_parts`.
-   `inserts_in_queue` (`UInt32`) - Number of inserts of blocks of data that need to be made. Insertions are usually replicated fairly quickly. If this number is large, it means something is wrong.
-   `merges_in_queue` (`UInt32`) - The number of merges waiting to be made. Sometimes merges are lengthy, so this value may be greater than zero for a long time.
-   `part_mutations_in_queue` (`UInt32`) - The number of mutations waiting to be made.
-   `queue_oldest_time` (`DateTime`) - If `queue_size` greater than 0, shows when the oldest operation was added to the queue.
-   `inserts_oldest_time` (`DateTime`) - See `queue_oldest_time`
-   `merges_oldest_time` (`DateTime`) - See `queue_oldest_time`
-   `part_mutations_oldest_time` (`DateTime`) - See `queue_oldest_time`

The next 4 columns have a non-zero value only where there is an active session with ZK.

-   `log_max_index` (`UInt64`) - Maximum entry number in the log of general activity.
-   `log_pointer` (`UInt64`) - Maximum entry number in the log of general activity that the replica copied to its execution queue, plus one. If `log_pointer` is much smaller than `log_max_index`, something is wrong.
-   `last_queue_update` (`DateTime`) - When the queue was updated last time.
-   `absolute_delay` (`UInt64`) - How big lag in seconds the current replica has.
-   `total_replicas` (`UInt8`) - The total number of known replicas of this table.
-   `active_replicas` (`UInt8`) - The number of replicas of this table that have a session in ZooKeeper (i.e., the number of functioning replicas).
-   `last_queue_update_exception` (`String`) - When the queue contains broken entries. Especially important when ClickHouse breaks backward compatibility between versions and log entries written by newer versions aren't parseable by old versions.
-   `zookeeper_exception` (`String`) - The last exception message, got if the error happened when fetching the info from ZooKeeper. 
-   `replica_is_active` ([Map(String, UInt8)](../../sql-reference/data-types/map.md)) — Map between replica name and is replica active.

If you request all the columns, the table may work a bit slowly, since several reads from ZooKeeper are made for each row.
If you do not request the last 4 columns (log_max_index, log_pointer, total_replicas, active_replicas), the table works quickly.

For example, you can check that everything is working correctly like this:

``` sql
SELECT
    database,
    table,
    is_leader,
    is_readonly,
    is_session_expired,
    future_parts,
    parts_to_check,
    columns_version,
    queue_size,
    inserts_in_queue,
    merges_in_queue,
    log_max_index,
    log_pointer,
    total_replicas,
    active_replicas
FROM system.replicas
WHERE
       is_readonly
    OR is_session_expired
    OR future_parts > 20
    OR parts_to_check > 10
    OR queue_size > 20
    OR inserts_in_queue > 10
    OR log_max_index - log_pointer > 10
    OR total_replicas < 2
    OR active_replicas < total_replicas
```

If this query does not return anything, it means that everything is fine.

[Original article](https://clickhouse.com/docs/en/operations/system-tables/replicas) <!--hide-->

