-- Tags: no-parallel-replicas
-- Regression test for plan-based parallel replicas over a top-level UNION ALL (a root UnionStep).
-- applyParallelReplicas must descend into the root union and distribute EVERY branch as one fragment,
-- not just the first (findReadingSteps, the view-expansion helper, skips a root union). Results must
-- match non-parallel execution. See PR #111063 review.

DROP TABLE IF EXISTS t_pr_tlu_1;
DROP TABLE IF EXISTS t_pr_tlu_2;

CREATE TABLE t_pr_tlu_1 (a UInt64) ENGINE = MergeTree ORDER BY a;
CREATE TABLE t_pr_tlu_2 (a UInt64) ENGINE = MergeTree ORDER BY a;
INSERT INTO t_pr_tlu_1 SELECT number FROM numbers(1000);
INSERT INTO t_pr_tlu_2 SELECT number + 1000 FROM numbers(1000);

SET enable_analyzer = 1;
SET enable_parallel_replicas = 1;
SET parallel_replicas_for_non_replicated_merge_tree = 1;
SET max_parallel_replicas = 3;
SET cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';
SET parallel_replicas_plan_based = 1;
SET parallel_replicas_local_plan = 1;
SET automatic_parallel_replicas_mode = 0;

-- Correctness: results match non-parallel.
SELECT count(), sum(a), min(a), max(a) FROM (SELECT a FROM t_pr_tlu_1 UNION ALL SELECT a FROM t_pr_tlu_2);

-- Plan shape: the whole root union is distributed -- one remote parallel-replicas read, no leftover split.
SELECT
    countIf(explain LIKE '%ParallelReplicasSplit%') > 0 AS has_split,
    countIf(explain LIKE '%ReadFromParallelReplicas%') > 0 AS has_remote_read
FROM (EXPLAIN optimize = 1, description = 0 SELECT a FROM t_pr_tlu_1 UNION ALL SELECT a FROM t_pr_tlu_2);

DROP TABLE t_pr_tlu_1;
DROP TABLE t_pr_tlu_2;
