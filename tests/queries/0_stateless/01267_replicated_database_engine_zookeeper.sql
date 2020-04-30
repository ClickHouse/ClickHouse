DROP DATABASE IF EXISTS test_db1;
DROP DATABASE IF EXISTS test_db2;
DROP TABLE IF EXISTS test_table1;
DROP TABLE IF EXISTS test_table2;

CREATE DATABASE test_db1 ENGINE = Replicated('/clickhouse/databases/test1', 'id1');
USE test_db1;
CREATE TABLE test_table1  (d Date, k UInt64, i32 Int32) ENGINE=ReplicatedMergeTree('/clickhouse/tables/test1', 'id1', d, k, 8192);

CREATE DATABASE test_db2 ENGINE = Replicated('/clickhouse/databases/test1', 'id2');
USE test_db2;
CREATE TABLE test_table2 (d Date, k UInt64, i32 Int32) ENGINE=ReplicatedMergeTree('/clickhouse/tables/test1', 'id2', d, k, 8192);
