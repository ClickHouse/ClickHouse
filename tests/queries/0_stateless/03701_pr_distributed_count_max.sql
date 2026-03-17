DROP TABLE IF EXISTS test_shard_scope SYNC;
DROP TABLE IF EXISTS dis_test_shard_scope SYNC;

SET enable_parallel_replicas=1, max_parallel_replicas=3;

CREATE TABLE test_shard_scope (`time_col` DateTime) ENGINE=ReplicatedMergeTree('/clickhouse/{database}/tables/test_shard_scope', '0') PARTITION BY tuple() ORDER BY time_col;
CREATE TABLE dis_test_shard_scope ENGINE=Distributed(test_cluster_one_shard_three_replicas_localhost, currentDatabase(), test_shard_scope);

INSERT INTO test_shard_scope (`time_col`) VALUES ('2025-10-23 10:26:46'), ('2025-10-23 10:26:47');

SELECT count(), max(time_col) from dis_test_shard_scope;

DROP TABLE test_shard_scope SYNC;
DROP TABLE dis_test_shard_scope SYNC;
