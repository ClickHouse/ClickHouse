-- TODO(#111876): plan-based parallel replicas cannot yet ship a FutureSetFromSubquery, so a
-- `WHERE x IN (SELECT ...)` predicate makes the outer read run locally instead of being distributed
-- (correct results, no parallel replicas). Before the fix this deterministically threw a LOGICAL_ERROR
-- "Cannot serialize FutureSetFromSubquery with no query plan" (with parallel_replicas_local_plan = 0)
-- or "Next task callback is not set" (#111677, with parallel_replicas_local_plan = 1).
-- Once the subquery set can be shipped, flip the plan-shape assertion below to expect the outer read to
-- be distributed for the IN (subquery) query.

DROP TABLE IF EXISTS t_pr_in_subquery;

CREATE TABLE t_pr_in_subquery (v Int64) ENGINE = MergeTree ORDER BY v;
INSERT INTO t_pr_in_subquery SELECT number FROM numbers(10);

SET enable_analyzer = 1;
SET enable_parallel_replicas = 1;
SET parallel_replicas_for_non_replicated_merge_tree = 1;
SET max_parallel_replicas = 3;
SET cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';
SET parallel_replicas_plan_based = 1;
SET automatic_parallel_replicas_mode = 0;

-- Correctness: the IN (subquery) query returns the same rows as without parallel replicas, for both
-- local-plan modes (previously a LOGICAL_ERROR / #111677).
SET parallel_replicas_local_plan = 0;
SELECT v FROM t_pr_in_subquery WHERE v IN (SELECT v FROM t_pr_in_subquery) ORDER BY v;

SET parallel_replicas_local_plan = 1;
SELECT v FROM t_pr_in_subquery WHERE v IN (SELECT v FROM t_pr_in_subquery) ORDER BY v;

-- Regression guard against over-disabling: a plain tuple IN (FutureSetFromTuple, no subquery set) must
-- STILL be distributed via a remote parallel-replicas read.
SET parallel_replicas_local_plan = 0;
SELECT countIf(explain LIKE '%ReadFromParallelReplicas%') > 0 AS tuple_in_is_distributed
FROM (EXPLAIN optimize = 1, description = 0 SELECT v FROM t_pr_in_subquery WHERE v IN (1, 2, 3));

DROP TABLE t_pr_in_subquery;
