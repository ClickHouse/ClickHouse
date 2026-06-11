DROP TABLE IF EXISTS test_unexpected_cluster;
CREATE TABLE test_unexpected_cluster (n UInt64) ENGINE=MergeTree() ORDER BY tuple();
INSERT INTO test_unexpected_cluster SELECT * FROM numbers(10);

SET parallel_replicas_only_with_analyzer = 0;  -- necessary for CI run with disabled analyzer

SET enable_parallel_replicas=2, max_parallel_replicas=2, cluster_for_parallel_replicas='test_cluster_two_shards', parallel_replicas_for_non_replicated_merge_tree=1;
SELECT count() FROM test_unexpected_cluster WHERE NOT ignore(*); -- { serverError UNEXPECTED_CLUSTER }

DROP TABLE test_unexpected_cluster;
