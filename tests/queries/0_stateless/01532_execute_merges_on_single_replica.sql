DROP TABLE IF EXISTS execute_on_single_replica_r1 NO DELAY;
DROP TABLE IF EXISTS execute_on_single_replica_r2 NO DELAY;

/* that test requires fixed zookeeper path */
CREATE TABLE execute_on_single_replica_r1 (x UInt64) ENGINE=ReplicatedMergeTree('/clickhouse/tables/test_01532/execute_on_single_replica', 'r1') ORDER BY tuple() SETTINGS execute_merges_on_single_replica_time_threshold=10;
CREATE TABLE execute_on_single_replica_r2 (x UInt64) ENGINE=ReplicatedMergeTree('/clickhouse/tables/test_01532/execute_on_single_replica', 'r2') ORDER BY tuple() SETTINGS execute_merges_on_single_replica_time_threshold=10;

INSERT INTO execute_on_single_replica_r1 VALUES (1);
SYSTEM SYNC REPLICA execute_on_single_replica_r2;

SET optimize_throw_if_noop=1;

SELECT '****************************';
SELECT '*** emulate normal feature operation - merges are distributed between replicas';

/* all_0_0_1 - will be merged by r1, and downloaded by r2 */
OPTIMIZE TABLE execute_on_single_replica_r1 FINAL;
SYSTEM SYNC REPLICA execute_on_single_replica_r2;

/* all_0_0_2 - will be merged by r1, and downloaded by r2 */
OPTIMIZE TABLE execute_on_single_replica_r2 FINAL;
SYSTEM SYNC REPLICA execute_on_single_replica_r1;

/* all_0_0_3 - will be merged by r2, and downloaded by r1 */
OPTIMIZE TABLE execute_on_single_replica_r1 FINAL;
SYSTEM SYNC REPLICA execute_on_single_replica_r2;

/* all_0_0_4 - will be merged by r2, and downloaded by r1 */
OPTIMIZE TABLE execute_on_single_replica_r2 FINAL;
SYSTEM SYNC REPLICA execute_on_single_replica_r1;

SELECT '****************************';
SELECT '*** emulate execute_merges_on_single_replica_time_threshold timeout';

SYSTEM STOP REPLICATION QUEUES execute_on_single_replica_r2;

/* all_0_0_5 - should be merged by r2, but it has replication queue stopped, so r1 do the merge */
OPTIMIZE TABLE execute_on_single_replica_r1 FINAL SETTINGS replication_alter_partitions_sync=0;

/* if we will check immediately we can find the log entry unchecked */
SELECT * FROM numbers(4) where sleepEachRow(1);

SELECT '****************************';
SELECT '*** timeout not exceeded, r1 waits for r2';

/* we can now check that r1 waits for r2 */
SELECT
    table,
    type,
    new_part_name,
    num_postponed > 0 AS has_postpones,
    postpone_reason
FROM system.replication_queue
WHERE table LIKE 'execute\\_on\\_single\\_replica\\_r%'
AND database = currentDatabase()
ORDER BY table
FORMAT Vertical;

/* we have execute_merges_on_single_replica_time_threshold exceeded */
SELECT * FROM numbers(10) where sleepEachRow(1);

SELECT '****************************';
SELECT '*** timeout exceeded, r1 failed to get the merged part from r2 and did the merge by its own';

SELECT
    table,
    type,
    new_part_name,
    num_postponed > 0 AS has_postpones,
    postpone_reason
FROM system.replication_queue
WHERE table LIKE 'execute\\_on\\_single\\_replica\\_r%'
AND database = currentDatabase()
ORDER BY table
FORMAT Vertical;

SYSTEM START REPLICATION QUEUES execute_on_single_replica_r2;
SYSTEM SYNC REPLICA execute_on_single_replica_r2;

SELECT '****************************';
SELECT '*** queue unfreeze';

SELECT
    table,
    type,
    new_part_name,
    num_postponed > 0 AS has_postpones,
    postpone_reason
FROM system.replication_queue
WHERE table LIKE 'execute\\_on\\_single\\_replica\\_r%'
AND database = currentDatabase()
ORDER BY table
FORMAT Vertical;

SELECT '****************************';
SELECT '*** disable the feature';

ALTER TABLE execute_on_single_replica_r1 MODIFY SETTING execute_merges_on_single_replica_time_threshold=0;
ALTER TABLE execute_on_single_replica_r2 MODIFY SETTING execute_merges_on_single_replica_time_threshold=0;

/* all_0_0_6 - we disabled the feature, both replicas will merge */
OPTIMIZE TABLE execute_on_single_replica_r2 FINAL;
/* all_0_0_7 - same */
OPTIMIZE TABLE execute_on_single_replica_r1 FINAL;

SYSTEM SYNC REPLICA execute_on_single_replica_r1;
SYSTEM SYNC REPLICA execute_on_single_replica_r2;

SYSTEM FLUSH LOGS;

SELECT '****************************';
SELECT '*** part_log';
SELECT
    part_name,
    arraySort(groupArrayIf(table, event_type = 'MergeParts')) AS mergers,
    arraySort(groupArrayIf(table, event_type = 'DownloadPart')) AS fetchers
FROM system.part_log
WHERE (event_time > (now() - 120))
  AND (table LIKE 'execute\\_on\\_single\\_replica\\_r%')
  AND (part_name NOT LIKE '%\\_0')
  AND (database = currentDatabase())
GROUP BY part_name
ORDER BY part_name
FORMAT Vertical;

DROP TABLE execute_on_single_replica_r1 NO DELAY;
DROP TABLE execute_on_single_replica_r2 NO DELAY;