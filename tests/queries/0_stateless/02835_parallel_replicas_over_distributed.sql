-- 1 shard

SELECT '-- 1 shard, 3 replicas';
DROP TABLE IF EXISTS test_d;
DROP TABLE IF EXISTS test;
CREATE TABLE test (id UInt64, date Date)
ENGINE = MergeTree
ORDER BY id;

CREATE TABLE IF NOT EXISTS test_d as test
ENGINE = Distributed(test_cluster_one_shard_three_replicas_localhost, currentDatabase(), test);

insert into test select *, today() from numbers(100);

SET enable_parallel_replicas = 2, max_parallel_replicas = 3, parallel_replicas_for_non_replicated_merge_tree=1;
SET parallel_replicas_only_with_analyzer = 0;  -- necessary for CI run with disabled analyzer

SELECT count(), min(id), max(id), avg(id)
FROM test_d;

insert into test select *, today() from numbers(100);

SELECT count(), min(id), max(id), avg(id)
FROM test_d;

-- The 2-shard case (test_cluster_two_shard_three_replicas_localhost) is covered by
-- the integration test test_parallel_replicas_over_distributed, because in stateless
-- tests the second shard uses addresses 127.0.0.{4-6} which the local server cannot
-- identify as itself, causing INCONSISTENT_CLUSTER_DEFINITION.
