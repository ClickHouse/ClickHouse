-- Tags: no-random-settings, no-random-merge-tree-settings

-- The initiator-local fragment of a parallel-replicas read must be optimized under the settings the
-- fragment is shipped with, because the remote replicas re-plan that same fragment under its own
-- `SETTINGS`. `optimizeTree` therefore overrides, on `ReadFromLocalParallelReplicaStep`, exactly the
-- optimizer gates that can end up calling `ReadFromMergeTree::requestReadingInOrder` - the call that
-- decides the coordination mode (`Default` / `WithOrder` / `ReverseOrder`) announced to the shared
-- coordinator. `query_plan_top_k_through_join` is one of them, because `topKThroughJoin` injects a
-- preserved-side `Sort + Limit` and re-optimizes that subtree, and that injected sort is what would
-- make the later read-in-order pass install an in-order read on the fragment.
--
-- Today that can only be a latent divergence: `topKThroughJoin` bails out up front when the
-- preserved input is read with parallel replicas, precisely to avoid the
-- "Replica decided to read in Default mode, not in WithOrder" mismatch. This test pins both halves:
-- the shape really is one `topKThroughJoin` acts on (otherwise the parallel-replicas assertions
-- would be vacuous), and under parallel replicas the initiator-local fragment does not depend on
-- which side of the query the setting was written on.
--
-- `query_plan_read_in_order_through_join = 0` everywhere, so `topKThroughJoin` is the only path that
-- could produce an in-order read: the second-pass through-`JOIN` read-in-order is off.

DROP TABLE IF EXISTS t_topk_scope_l;
DROP TABLE IF EXISTS t_topk_scope_r;

CREATE TABLE t_topk_scope_l (k Int64, payload String) ENGINE = MergeTree ORDER BY k;
CREATE TABLE t_topk_scope_r (k Int64, value String) ENGINE = MergeTree ORDER BY k;

INSERT INTO t_topk_scope_l SELECT number, repeat('a', 8) FROM numbers(10000);
INSERT INTO t_topk_scope_r SELECT number, repeat('b', 8) FROM numbers(10000);

SET enable_analyzer = 1;
SET optimize_read_in_order = 1;
SET query_plan_read_in_order = 1;
SET query_plan_read_in_order_through_join = 0;
SET query_plan_join_swap_table = false;
SET query_plan_max_limit_for_top_k_optimization = 0;
SET enable_join_runtime_filters = 0;
SET enable_lazy_columns_replication = 0;
SET query_plan_optimize_lazy_materialization = 0;
SET max_bytes_before_external_join = 0;
SET max_bytes_ratio_before_external_join = 0;

-- Baseline without parallel replicas: `topKThroughJoin` does act on this shape, adding a second
-- `Sorting` (the injected preserved-side `Sort + Limit`) on top of the query's own one.
SET enable_parallel_replicas = 0;

SELECT 'baseline_top_k_off';
SELECT countIf(explain LIKE '%Sorting%') FROM (
    EXPLAIN actions = 0
    SELECT l.k AS k, r.value AS value FROM t_topk_scope_l AS l LEFT JOIN t_topk_scope_r AS r ON r.k = l.k
    ORDER BY l.k DESC LIMIT 10
    SETTINGS query_plan_top_k_through_join = 0
);

SELECT 'baseline_top_k_on';
SELECT countIf(explain LIKE '%Sorting%') FROM (
    EXPLAIN actions = 0
    SELECT l.k AS k, r.value AS value FROM t_topk_scope_l AS l LEFT JOIN t_topk_scope_r AS r ON r.k = l.k
    ORDER BY l.k DESC LIMIT 10
    SETTINGS query_plan_top_k_through_join = 1
);

SET automatic_parallel_replicas_mode = 0;
SET enable_parallel_replicas = 1;
SET parallel_replicas_for_non_replicated_merge_tree = 1;
SET max_parallel_replicas = 3;
SET cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';
SET parallel_replicas_local_plan = 1;

-- The outer query and the shipped subquery disagree about `query_plan_top_k_through_join`, in both
-- directions. The initiator-local fragment must come out the same either way: no injected
-- preserved-side `Sort + Limit`, hence no read-in-order the remote replicas would not also install.
SET query_plan_top_k_through_join = 0;

SELECT 'pr_local_subquery_enables_top_k';
SELECT countIf(explain LIKE '%Sorting%') FROM (
    EXPLAIN actions = 0
    SELECT k, value FROM (
        SELECT l.k AS k, r.value AS value FROM t_topk_scope_l AS l LEFT JOIN t_topk_scope_r AS r ON r.k = l.k
        ORDER BY l.k DESC LIMIT 10
        SETTINGS query_plan_top_k_through_join = 1
    )
);

SET query_plan_top_k_through_join = 1;

SELECT 'pr_local_subquery_disables_top_k';
SELECT countIf(explain LIKE '%Sorting%') FROM (
    EXPLAIN actions = 0
    SELECT k, value FROM (
        SELECT l.k AS k, r.value AS value FROM t_topk_scope_l AS l LEFT JOIN t_topk_scope_r AS r ON r.k = l.k
        ORDER BY l.k DESC LIMIT 10
        SETTINGS query_plan_top_k_through_join = 0
    )
);

-- Whichever value the fragment is built under, the answer must not change.
SELECT 'correctness';
SELECT count(), max(k), min(k) FROM (
    SELECT k, value FROM (
        SELECT l.k AS k, r.value AS value FROM t_topk_scope_l AS l LEFT JOIN t_topk_scope_r AS r ON r.k = l.k
        ORDER BY l.k DESC LIMIT 10
        SETTINGS query_plan_top_k_through_join = 1
    )
);
SELECT count(), max(k), min(k) FROM (
    SELECT k, value FROM (
        SELECT l.k AS k, r.value AS value FROM t_topk_scope_l AS l LEFT JOIN t_topk_scope_r AS r ON r.k = l.k
        ORDER BY l.k DESC LIMIT 10
        SETTINGS query_plan_top_k_through_join = 0
    )
);

DROP TABLE t_topk_scope_l;
DROP TABLE t_topk_scope_r;
