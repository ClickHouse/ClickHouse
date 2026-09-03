-- Tags: zookeeper, no-replicated-database, no-ordinary-database
-- no-replicated-database: we explicitly run this test by creating a replicated database test_03321

DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE:Identifier};

CREATE DATABASE {CLICKHOUSE_DATABASE:Identifier} ENGINE=Replicated('/clickhouse/databases/{database}', 'shard1', 'replica1');

USE {CLICKHOUSE_DATABASE:Identifier};

CREATE TABLE t1 (x UInt8, y String) ENGINE=ReplicatedMergeTree ORDER BY x;

-- Test that `database_replicated_allow_replicated_engine_arguments=0` for *MergeTree tables in Replicated databases work as expected for `CREATE TABLE ... AS ...` queries.
-- While creating t2 with the table structure of t1, the AST for the create query would contain the engine args from t1. For this kind of queries, skip this validation if
-- the value of the arguments match the default values and also skip throwing an exception.
SET database_replicated_allow_replicated_engine_arguments=0;
CREATE TABLE t2 AS t1;

SHOW CREATE TABLE t2;

DROP DATABASE {CLICKHOUSE_DATABASE:Identifier};
