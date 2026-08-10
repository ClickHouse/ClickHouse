-- Plan-based parallel replicas distribute a simple `IN (subquery)` by shipping the subquery plan: the
-- shipped fragment keeps the set's query plan (FutureSetFromSubquery::source is preserved through the
-- in-place index build and serialized while alive), and each replica rebuilds the set over its own
-- replicated data. Before the fix this threw LOGICAL_ERROR "Cannot serialize FutureSetFromSubquery with
-- no query plan" (#111876). Covers both the non-primary-key IN and the primary-key IN (which is built
-- in-place for index analysis) - both must be distributed now, not run locally.

DROP TABLE IF EXISTS t_pr_in_subquery;

CREATE TABLE t_pr_in_subquery (k Int64, v Int64) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_pr_in_subquery SELECT number, number FROM numbers(10);

SET enable_analyzer = 1;
SET enable_parallel_replicas = 1;
SET parallel_replicas_for_non_replicated_merge_tree = 1;
SET max_parallel_replicas = 3;
SET cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';
SET parallel_replicas_plan_based = 1;
SET automatic_parallel_replicas_mode = 0;

-- Correctness: IN (subquery) returns the same rows as without parallel replicas, for both local-plan
-- modes (previously a LOGICAL_ERROR).
SET parallel_replicas_local_plan = 0;
SELECT v FROM t_pr_in_subquery WHERE v IN (SELECT v FROM t_pr_in_subquery WHERE v % 2 = 0) ORDER BY v;
SET parallel_replicas_local_plan = 1;
SELECT v FROM t_pr_in_subquery WHERE v IN (SELECT v FROM t_pr_in_subquery WHERE v % 2 = 0) ORDER BY v;

-- Non-primary-key IN (subquery) is distributed: the read is shipped to the replicas.
SET parallel_replicas_local_plan = 0;
SELECT countIf(explain LIKE '%ReadFromParallelReplicas%') > 0 AS non_pk_in_distributed
FROM (EXPLAIN optimize = 1, description = 0 SELECT v FROM t_pr_in_subquery WHERE v IN (SELECT v FROM t_pr_in_subquery WHERE v % 2 = 0));

-- Primary-key IN (subquery) - built in-place for index analysis - is also distributed now.
SELECT countIf(explain LIKE '%ReadFromParallelReplicas%') > 0 AS pk_in_distributed
FROM (EXPLAIN optimize = 1, description = 0 SELECT v FROM t_pr_in_subquery WHERE k IN (SELECT k FROM t_pr_in_subquery WHERE k % 2 = 0));

-- The same holds with a local plan on the initiator (`parallel_replicas_local_plan = 1`), where the plan
-- also keeps the initiator's own `ReadFromMergeTree`.
SET parallel_replicas_local_plan = 1;
SELECT countIf(explain LIKE '%ReadFromParallelReplicas%') > 0 AS non_pk_in_distributed_local_plan
FROM (EXPLAIN optimize = 1, description = 0 SELECT v FROM t_pr_in_subquery WHERE v IN (SELECT v FROM t_pr_in_subquery WHERE v % 2 = 0));

-- Regression guard: a plain tuple IN (no subquery set) stays distributed.
SET parallel_replicas_local_plan = 0;
SELECT countIf(explain LIKE '%ReadFromParallelReplicas%') > 0 AS tuple_in_distributed
FROM (EXPLAIN optimize = 1, description = 0 SELECT v FROM t_pr_in_subquery WHERE k IN (1, 2, 3));

DROP TABLE t_pr_in_subquery;
