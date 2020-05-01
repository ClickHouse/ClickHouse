DROP DATABASE IF EXISTS test_db1;
DROP DATABASE IF EXISTS test_db2;

CREATE DATABASE test_db1 ENGINE = Replicated('/clickhouse/databases/test1', 'id1');
CREATE TABLE test_db1.replicated_table  (d Date, k UInt64, i32 Int32) ENGINE=ReplicatedMergeTree('/clickhouse/tables/test1', 'id1', d, k, 8192);
CREATE TABLE test_db1.basic_table (EventDate Date, CounterID Int) engine=MergeTree(EventDate, (CounterID, EventDate), 8192);

CREATE DATABASE test_db2 ENGINE = Replicated('/clickhouse/databases/test1', 'id2');
CREATE TABLE test_db2.replicated_table (d Date, k UInt64, i32 Int32) ENGINE=ReplicatedMergeTree('/clickhouse/tables/test1', 'id2', d, k, 8192);
CREATE TABLE test_db2.basic_table (EventDate Date, CounterID Int) engine=MergeTree(EventDate, (CounterID, EventDate), 8192);
