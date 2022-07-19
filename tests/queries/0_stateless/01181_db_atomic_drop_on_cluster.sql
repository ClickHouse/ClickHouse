DROP TABLE IF EXISTS test_repl ON CLUSTER test_shard_localhost SYNC;
CREATE TABLE test_repl ON CLUSTER test_shard_localhost (n UInt64) ENGINE ReplicatedMergeTree('/clickhouse/test_01181/{database}/test_repl','r1') ORDER BY tuple();
DETACH TABLE test_repl ON CLUSTER test_shard_localhost SYNC;
ATTACH TABLE test_repl ON CLUSTER test_shard_localhost;
DROP TABLE test_repl ON CLUSTER test_shard_localhost SYNC;
