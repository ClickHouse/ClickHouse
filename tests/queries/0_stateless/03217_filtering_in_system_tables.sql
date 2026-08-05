-- If filtering is not done correctly on databases, then this query report to read 3 rows, which are: `system.tables`, `information_schema.tables` and `INFORMATION_SCHEMA.tables`
SELECT database, table FROM system.tables WHERE database = 'information_schema' AND table = 'tables';

CREATE TABLE test_03217_system_tables_replica_1(x UInt32)
    ENGINE ReplicatedMergeTree('/clickhouse/tables/{database}/test_03217_system_tables_replica', 'r1')
    ORDER BY x;
CREATE TABLE test_03217_system_tables_replica_2(x UInt32)
    ENGINE ReplicatedMergeTree('/clickhouse/tables/{database}/test_03217_system_tables_replica', 'r2')
    ORDER BY x;

-- Make sure we can read both replicas
-- The replica name might be altered because of `_functional_tests_helper_database_replicated_replace_args_macros`,
-- thus we need to use `left`
SELECT 'both', database, table, left(replica_name, 2) FROM system.replicas WHERE database = currentDatabase();
-- If filtering is not done correctly on database-table column, then this query report to read 2 rows, which are the above tables
SELECT database, table, left(replica_name, 2) FROM system.replicas WHERE database = currentDatabase() AND table = 'test_03217_system_tables_replica_1' AND replica_name LIKE 'r1%';
SYSTEM FLUSH LOGS query_log;
-- argMax is necessary to make the test repeatable

-- StorageSystemTables
SELECT argMax(read_rows, event_time_microseconds) FROM system.query_log WHERE 1
    AND current_database = currentDatabase()
    AND query LIKE '%SELECT database, table FROM system.tables WHERE database = \'information_schema\' AND table = \'tables\';'
    AND type = 'QueryFinish';

-- StorageSystemReplicas
SELECT argMax(read_rows, event_time_microseconds) FROM system.query_log WHERE 1
    AND current_database = currentDatabase()
    AND query LIKE '%SELECT database, table, left(replica_name, 2) FROM system.replicas WHERE database = currentDatabase() AND table = \'test_03217_system_tables_replica_1\' AND replica_name LIKE \'r1\%\';'
    AND type = 'QueryFinish';
