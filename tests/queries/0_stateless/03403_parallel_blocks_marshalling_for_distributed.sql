set serialize_query_plan = 0;

CREATE TABLE t(a UInt64, b UInt64) ENGINE = MergeTree ORDER BY a;

INSERT INTO t SELECT
    number,
    number
FROM numbers_mt(1000000);

SET enable_parallel_replicas = 1, parallel_replicas_local_plan = 1, max_parallel_replicas = 3, cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost', parallel_replicas_for_non_replicated_merge_tree = 1;
SET prefer_localhost_replica = 1, enable_analyzer = 1;

SELECT replaceRegexpAll(explain, 'ReadFromRemoteParallelReplicas.*', 'ReadFromRemoteParallelReplicas')
FROM (
   EXPLAIN distributed = 1
    SELECT a
      FROM t
  GROUP BY a
);

SELECT replaceRegexpAll(explain, 'ReadFromRemoteParallelReplicas.*', 'ReadFromRemoteParallelReplicas')
FROM (
   EXPLAIN distributed = 1
    SELECT a
      FROM t
);

-- This test case triggers a logical error in fuzzing tests. The issue existed on master prior to this PR. See https://github.com/ClickHouse/ClickHouse/pull/81610
--SELECT replaceRegexpAll(explain, 'ReadFromRemoteParallelReplicas.*', 'ReadFromRemoteParallelReplicas')
--FROM (
--   EXPLAIN distributed = 1
--    SELECT a
--      FROM remote('127.0.0.{1,2}', currentDatabase(), t)
--  GROUP BY a
--);

-- Not yet supported; see the comment in `Planner::buildPlanForQueryNode()`
--SELECT replaceRegexpAll(explain, 'ReadFromRemoteParallelReplicas.*', 'ReadFromRemoteParallelReplicas')
--FROM (
--   EXPLAIN distributed = 1
--    SELECT a
--      FROM remote('127.0.0.{1,2}', default.t)
--);

