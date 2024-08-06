SELECT database, table FROM system.tables WHERE database = 'information_schema' AND table = 'tables';

-- To verify StorageSystemReplicas applies the filter properly
CREATE TABLE test_replica_1(x UInt32)
    ENGINE ReplicatedMergeTree('/clickhouse/tables/{database}/test_03217/test_replica', 'r1')
    ORDER BY x;
CREATE TABLE test_replica_2(x UInt32)
    ENGINE ReplicatedMergeTree('/clickhouse/tables/{database}/test_03217/test_replica', 'r2')
    ORDER BY x;

SELECT database, table, replica_name FROM system.replicas WHERE database = currentDatabase() AND table = 'test_replica_1' AND replica_name = 'r1';


-- To verify StorageMerge
CREATE TABLE all_replicas (x UInt32)
    ENGINE = Merge(currentDatabase(), 'test_replica_*');

INSERT INTO test_replica_1 SELECT number AS x FROM numbers(10);
SYSTEM SYNC REPLICA test_replica_2;
-- If the filter not applied, then the plan will show both replicas
EXPLAIN SELECT _table, count() FROM all_replicas WHERE  _table = 'test_replica_1' AND x >= 0 GROUP BY _table;

SYSTEM FLUSH LOGS;
-- argMin-argMax make the test repeatable

-- StorageSystemTables
SELECT argMin(read_rows, event_time_microseconds), argMax(read_rows, event_time_microseconds) FROM system.query_log WHERE 1
    AND query LIKE '%SELECT database, table FROM system.tables WHERE database = \'information_schema\' AND table = \'tables\';'
    AND type = 'QueryFinish';

-- StorageSystemReplicas
SELECT argMin(read_rows, event_time_microseconds), argMax(read_rows, event_time_microseconds) FROM system.query_log WHERE 1
    AND query LIKE '%SELECT database, table, replica_name FROM system.replicas WHERE database = currentDatabase() AND table = \'test_replica_1\' AND replica_name = \'r1\';'
    AND type = 'QueryFinish';
