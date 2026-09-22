-- TODO(#111876): plan-based parallel replicas cannot yet ship a FutureSetFromSubquery, so an
-- `IN (subquery)` predicate must keep the outer read LOCAL instead of distributing it. If the outer read
-- were distributed, serializing the shipped fragment would throw LOGICAL_ERROR "Cannot serialize
-- FutureSetFromSubquery with no query plan" (parallel_replicas_local_plan = 0) or "Next task callback is
-- not set" (#111677, parallel_replicas_local_plan = 1). The EXPLAIN assertions below pin that contract:
-- the outer read must NOT appear as ReadFromParallelReplicas. Once the subquery set can be shipped
-- (#111876), flip them to expect the outer read to be distributed.

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

-- Correctness: IN (subquery) returns the same rows as without parallel replicas, for both local-plan
-- modes (previously a LOGICAL_ERROR / #111677).
SET parallel_replicas_local_plan = 0;
SELECT v FROM t_pr_in_subquery WHERE v IN (SELECT v FROM t_pr_in_subquery) ORDER BY v;

SET parallel_replicas_local_plan = 1;
SELECT v FROM t_pr_in_subquery WHERE v IN (SELECT v FROM t_pr_in_subquery) ORDER BY v;

-- Contract: the outer read of the IN (subquery) is NOT distributed; it stays a local ReadFromMergeTree
-- (no ReadFromParallelReplicas). The subquery appears as a reference in the plan, so this count reflects
-- only the outer read. Asserted for both local-plan modes. Flip to expect distribution once #111876 lands.
SET parallel_replicas_local_plan = 0;
SELECT
    countIf(explain LIKE '%ReadFromParallelReplicas%') AS outer_read_distributed,
    countIf(explain LIKE '%ReadFromMergeTree%') > 0 AS outer_read_local
FROM (EXPLAIN optimize = 1, description = 0 SELECT v FROM t_pr_in_subquery WHERE v IN (SELECT v FROM t_pr_in_subquery));

SET parallel_replicas_local_plan = 1;
SELECT
    countIf(explain LIKE '%ReadFromParallelReplicas%') AS outer_read_distributed,
    countIf(explain LIKE '%ReadFromMergeTree%') > 0 AS outer_read_local
FROM (EXPLAIN optimize = 1, description = 0 SELECT v FROM t_pr_in_subquery WHERE v IN (SELECT v FROM t_pr_in_subquery));

-- Regression guard against over-disabling: a plain tuple IN (no subquery set) MUST still be distributed.
SET parallel_replicas_local_plan = 0;
SELECT countIf(explain LIKE '%ReadFromParallelReplicas%') > 0 AS tuple_in_distributed
FROM (EXPLAIN optimize = 1, description = 0 SELECT v FROM t_pr_in_subquery WHERE v IN (1, 2, 3));

DROP TABLE t_pr_in_subquery;
