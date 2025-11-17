DROP TABLE IF EXISTS test_shard_scope;
DROP TABLE IF EXISTS dis_test_shard_scope;

SET parallel_replicas_only_with_analyzer = 0;  -- necessary for CI run with disabled analyzer
SET serialize_query_plan = 0;
SET enable_parallel_replicas=1, max_parallel_replicas=3, parallel_replicas_for_non_replicated_merge_tree=1, cluster_for_parallel_replicas='test_cluster_one_shard_three_replicas_localhost';

CREATE TABLE test_shard_scope (`time_col` DateTime) ENGINE=MergeTree() ORDER BY time_col;
CREATE TABLE dis_test_shard_scope ENGINE=Distributed(test_cluster_one_shard_three_replicas_localhost, currentDatabase(), test_shard_scope);

INSERT INTO test_shard_scope (`time_col`) VALUES ('2025-10-23 10:26:46'), ('2025-10-23 10:26:47');

SELECT count(), max(time_col) from dis_test_shard_scope;

DROP TABLE test_shard_scope;
DROP TABLE dis_test_shard_scope;
