DROP TABLE IF EXISTS test;
CREATE TABLE test (x UInt8) ENGINE = MergeTree ORDER BY x;
INSERT INTO test VALUES (1), (2), (3);

SET enable_parallel_replicas = 1, max_parallel_replicas = 2, cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost', prefer_localhost_replica = 0, parallel_replicas_for_non_replicated_merge_tree = 1;

WITH (SELECT '111111111111111111111111111111111111111'::UInt128) AS v SELECT sum(x), max(v) FROM test;

DROP TABLE test;
