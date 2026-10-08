-- A row policy carrying `IN (subquery)` reaches the `MergeTree` read as its row-level filter, which
-- `ReadFromMergeTree::serialize` ships to the replicas just like `PREWHERE`. When that set took the
-- destructive in-place build (its subquery source is non-clonable - here a nested `IN`), its query plan is
-- gone, and shipping the fragment threw `Cannot serialize FutureSetFromSubquery with no query plan`.
-- Such a fragment has to stay local instead.

DROP ROW POLICY IF EXISTS t_rp_pol ON t_rp_main;
DROP ROW POLICY IF EXISTS t_rp_pol_plain ON t_rp_main;
DROP TABLE IF EXISTS t_rp_main;
DROP TABLE IF EXISTS t_rp_allow;
DROP TABLE IF EXISTS t_rp_nested;

CREATE TABLE t_rp_main (k Int64, v Int64) ENGINE = MergeTree ORDER BY k;
CREATE TABLE t_rp_allow (k Int64) ENGINE = MergeTree ORDER BY k;
CREATE TABLE t_rp_nested (k Int64) ENGINE = MergeTree ORDER BY k;

INSERT INTO t_rp_main SELECT number, number FROM numbers(100);
INSERT INTO t_rp_allow SELECT number FROM numbers(50);
INSERT INTO t_rp_nested SELECT number FROM numbers(25);

-- The nested `IN` is what makes the policy's subquery source non-clonable, so the in-place build for index
-- analysis consumes the plan.
CREATE ROW POLICY t_rp_pol ON t_rp_main
    USING k IN (SELECT k FROM t_rp_allow WHERE k IN (SELECT k FROM t_rp_nested)) TO ALL;

SET enable_analyzer = 1;
SET enable_parallel_replicas = 1;
SET parallel_replicas_for_non_replicated_merge_tree = 1;
SET max_parallel_replicas = 3;
SET cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';
SET parallel_replicas_plan_based = 1;
SET automatic_parallel_replicas_mode = 0;

-- The policy keeps rows 0..24 (previously a LOGICAL_ERROR). `parallel_replicas_local_plan` is left to the
-- value CI randomizes it to.
SELECT count(), min(k), max(k) FROM t_rp_main;

-- The read stays local: the fragment cannot be serialized, so the split marker is left unconverted.
SELECT countIf(explain LIKE '%ReadFromParallelReplicas%') = 0 AS stayed_local
FROM (EXPLAIN optimize = 1, description = 0 SELECT count() FROM t_rp_main);

-- A policy whose subquery source is clonable keeps its plan and is still distributed.
DROP ROW POLICY t_rp_pol ON t_rp_main;
CREATE ROW POLICY t_rp_pol_plain ON t_rp_main USING k IN (SELECT k FROM t_rp_allow) TO ALL;

SELECT count(), min(k), max(k) FROM t_rp_main;
SELECT countIf(explain LIKE '%ReadFromParallelReplicas%') > 0 AS distributed
FROM (EXPLAIN optimize = 1, description = 0 SELECT count() FROM t_rp_main);

DROP ROW POLICY t_rp_pol_plain ON t_rp_main;
DROP TABLE t_rp_main;
DROP TABLE t_rp_allow;
DROP TABLE t_rp_nested;
