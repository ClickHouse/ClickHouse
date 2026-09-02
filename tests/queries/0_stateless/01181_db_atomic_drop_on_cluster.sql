-- Tags: no-replicated-database
-- Tag no-replicated-database: ON CLUSTER is not allowed

DROP TABLE IF EXISTS test_repl ON CLUSTER test_shard_localhost NO DELAY;
CREATE TABLE test_repl ON CLUSTER test_shard_localhost (n UInt64) ENGINE ReplicatedMergeTree('/clickhouse/test_01181/{database}/test_repl','r1') ORDER BY tuple();
DETACH TABLE test_repl ON CLUSTER test_shard_localhost NO DELAY;
ATTACH TABLE test_repl ON CLUSTER test_shard_localhost;
DROP TABLE test_repl ON CLUSTER test_shard_localhost NO DELAY;
