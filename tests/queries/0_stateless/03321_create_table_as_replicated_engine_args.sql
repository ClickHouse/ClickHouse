-- Tags: zookeeper, no-replicated-database, no-parallel, no-ordinary-database
-- no-replicated-database: we explicitly run this test by creating a replicated database test_03321

DROP DATABASE IF EXISTS test_03321;
CREATE DATABASE test_03321 ENGINE=Replicated('/clickhouse/databases/test_03321', 'shard1', 'replica1');

CREATE TABLE test_03321.t1 (x UInt8, y String) ENGINE=ReplicatedMergeTree ORDER BY x;

-- Test that `database_replicated_allow_replicated_engine_arguments=0` for *MergeTree tables in Replicated databases work as expected for `CREATE TABLE ... AS ...` queries.
-- While creating t2 with the table structure of t1, the AST for the create query would contain the engine args from t1. For this kind of queries, skip this validation if
-- the value of the arguments match the default values and also skip throwing an exception.
SET database_replicated_allow_replicated_engine_arguments=0;
CREATE TABLE test_03321.t2 AS test_03321.t1

SHOW CREATE TABLE test_03321.t2

DROP DATABASE test_03321;
