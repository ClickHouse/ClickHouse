SET explain_query_plan_default = 'legacy';
set serialize_query_plan = 0;
set enable_parallel_blocks_marshalling = 1; -- EXPLAIN output includes BlocksMarshalling node when enabled

CREATE TABLE t(a UInt64, b UInt64) ENGINE = MergeTree ORDER BY a;

INSERT INTO t SELECT
    number,
    number
FROM numbers_mt(1000000);

SET automatic_parallel_replicas_mode = 0;
SET enable_parallel_replicas = 1, parallel_replicas_local_plan = 1, max_parallel_replicas = 3, cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost', parallel_replicas_for_non_replicated_merge_tree = 1;
SET prefer_localhost_replica = 1;

-- These plans are the query-based ones: plan-based ships the fragment as a plan, so it carries the
-- `BlocksMarshalling` step inside the reading step's description instead of as its children.
-- `05238_pr_plan_based_blocks_marshalling` covers the same queries for plan-based.
SET parallel_replicas_plan_based = 0;

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

