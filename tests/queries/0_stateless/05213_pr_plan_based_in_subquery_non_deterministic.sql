-- An `IN (subquery)` set is shipped as its subquery plan and rebuilt by every replica, so the subquery has
-- to be deterministic: otherwise each replica compares its rows against a different set, which is wrong
-- rows rather than an exception. `rand()` reaches the replicas as a live function - unlike `now()` it is not
-- constant-folded, because folding needs a `ColumnConst` result - so such a fragment has to stay local.
-- `rand() >= 0` is always true, which keeps the result stable while leaving a `rand` node in the DAG.

DROP TABLE IF EXISTS t_nondet_in;

CREATE TABLE t_nondet_in (k Int64, v Int64) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_nondet_in SELECT number, number FROM numbers(100);

SET enable_analyzer = 1;
SET enable_parallel_replicas = 1;
SET parallel_replicas_for_non_replicated_merge_tree = 1;
SET max_parallel_replicas = 3;
SET cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';
SET parallel_replicas_plan_based = 1;
SET automatic_parallel_replicas_mode = 0;

-- The set is every `k`, so all 100 rows match. `parallel_replicas_local_plan` is left to the value CI
-- randomizes it to.
SELECT count() FROM t_nondet_in WHERE k IN (SELECT k FROM t_nondet_in WHERE rand() >= 0);

-- The fragment stays local: the split marker is left unconverted.
SELECT countIf(explain LIKE '%ReadFromParallelReplicas%') = 0 AS stayed_local
FROM (EXPLAIN optimize = 1, description = 0
    SELECT count() FROM t_nondet_in WHERE k IN (SELECT k FROM t_nondet_in WHERE rand() >= 0));

-- A deterministic subquery is still distributed.
SELECT countIf(explain LIKE '%ReadFromParallelReplicas%') > 0 AS distributed
FROM (EXPLAIN optimize = 1, description = 0
    SELECT count() FROM t_nondet_in WHERE k IN (SELECT k FROM t_nondet_in WHERE k % 2 = 0));

-- `now()` must NOT block shipping: it is deterministic within a query, so the initiator folds it and
-- serializes the folded value - every replica sees the same timestamp. Rejecting it would cost a lot of
-- parallelism for no correctness gain.
SELECT countIf(explain LIKE '%ReadFromParallelReplicas%') > 0 AS now_still_distributed
FROM (EXPLAIN optimize = 1, description = 0
    SELECT count() FROM t_nondet_in WHERE k IN (SELECT k FROM t_nondet_in WHERE toDateTime(k) < now()));

DROP TABLE t_nondet_in;
