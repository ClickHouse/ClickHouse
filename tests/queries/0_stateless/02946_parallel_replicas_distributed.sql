DROP TABLE IF EXISTS test;
DROP TABLE IF EXISTS test_d;

CREATE TABLE test (id UInt64, date Date)
ENGINE = MergeTree
ORDER BY id
AS select *, '2023-12-25' from numbers(100);

CREATE TABLE IF NOT EXISTS test_d as test
ENGINE = Distributed(test_cluster_one_shard_three_replicas_localhost, currentDatabase(), test);

SET parallel_replicas_only_with_analyzer = 0;  -- necessary for CI run with disabled analyzer

SELECT count(), sum(id)
FROM test_d
SETTINGS enable_parallel_replicas = 2, max_parallel_replicas = 3, parallel_replicas_for_non_replicated_merge_tree=1;

DROP TABLE test_d;
DROP TABLE test;
