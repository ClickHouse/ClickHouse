CREATE TABLE test_03217_merge_replica_1(x UInt32)
    ENGINE ReplicatedMergeTree('/clickhouse/tables/{database}/test_03217_merge_replica', 'r1')
    ORDER BY x;
CREATE TABLE test_03217_merge_replica_2(x UInt32)
    ENGINE ReplicatedMergeTree('/clickhouse/tables/{database}/test_03217_merge_replica', 'r2')
    ORDER BY x;


CREATE TABLE test_03217_all_replicas (x UInt32)
    ENGINE = Merge(currentDatabase(), 'test_03217_merge_replica_*');

INSERT INTO test_03217_merge_replica_1 SELECT number AS x FROM numbers(10);
SYSTEM SYNC REPLICA test_03217_merge_replica_2;

-- If the filter on _table is not applied, then the plan will show both replicas
EXPLAIN SELECT _table, count() FROM test_03217_all_replicas WHERE  _table = 'test_03217_merge_replica_1' AND x >= 0 GROUP BY _table SETTINGS enable_analyzer=1;
