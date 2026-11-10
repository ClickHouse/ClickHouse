-- Regression test: `topKThroughJoin` defers to second-pass `optimizeReadInOrder`
-- only when `query_plan_join_swap_table` is explicitly `false`. The deferral
-- runs in the first pass on the logical (or not-yet-optimized physical) join,
-- but `optimizeJoinLegacy` runs later and can swap `LEFT` to `RIGHT` via
-- `TableJoin::swapSides` when the swap is allowed. After the swap,
-- `optimizeReadInOrder` rejects the join (`isInnerOrLeft(JoinKind::Right)`
-- is false) and silently disables both optimizations. The gate makes the
-- deferral commit only when the join side is guaranteed stable.

SET enable_analyzer = 1;
SET query_plan_top_k_through_join = 1;

DROP TABLE IF EXISTS t_l;
DROP TABLE IF EXISTS t_r;

CREATE TABLE t_l (k Int64, payload String) ENGINE = MergeTree() ORDER BY k;
CREATE TABLE t_r (k Int64, value String) ENGINE = MergeTree() ORDER BY k;

INSERT INTO t_l SELECT number, repeat('a', 8) FROM numbers(1000);
INSERT INTO t_r SELECT number, repeat('b', 8) FROM numbers(1000);

-- `query_plan_join_swap_table = false`: deferral fires, the outer `Sort + Limit`
-- is satisfied by reading in order through the join. Expect one of each.
-- `max_bytes_*_before_external_join = 0` pins automatic spilling off so the
-- deferral's "can the second pass actually apply" check is not gated by
-- `SpillingHashJoin::hasDelayedBlocks()`.
SELECT 'swap_false' AS label, countIf(explain LIKE '%Sorting%') AS sort_count, countIf(explain LIKE '%Limit%') AS limit_count
FROM ( EXPLAIN actions = 0
    SELECT l.k, r.value FROM t_l AS l LEFT JOIN t_r AS r ON r.k = l.k
    ORDER BY l.k DESC LIMIT 10
    SETTINGS query_plan_join_swap_table = false,
             optimize_read_in_order = 1,
             query_plan_read_in_order = 1, query_plan_read_in_order_through_join = 1,
             query_plan_max_limit_for_top_k_optimization = 0,
             enable_join_runtime_filters = 0, enable_lazy_columns_replication = 0,
             query_plan_optimize_lazy_materialization = 0,
             enable_parallel_replicas = 0,
             max_bytes_before_external_join = 0, max_bytes_ratio_before_external_join = 0
);

-- `query_plan_join_swap_table = true`: the join may be swapped from `LEFT` to
-- `RIGHT`, after which the second pass would reject the join. Without the
-- gate, the deferral would silently disable both optimizations. With the
-- gate, `topKThroughJoin` fires and injects an inner `Sort + Limit` on the
-- preserved side. Expect two of each (outer pair + injected pair).
SELECT 'swap_true' AS label, countIf(explain LIKE '%Sorting%') AS sort_count, countIf(explain LIKE '%Limit%') AS limit_count
FROM ( EXPLAIN actions = 0
    SELECT l.k, r.value FROM t_l AS l LEFT JOIN t_r AS r ON r.k = l.k
    ORDER BY l.k DESC LIMIT 10
    SETTINGS query_plan_join_swap_table = true,
             optimize_read_in_order = 1,
             query_plan_read_in_order = 1, query_plan_read_in_order_through_join = 1,
             query_plan_max_limit_for_top_k_optimization = 0,
             enable_join_runtime_filters = 0, enable_lazy_columns_replication = 0,
             query_plan_optimize_lazy_materialization = 0,
             enable_parallel_replicas = 0,
             max_bytes_before_external_join = 0, max_bytes_ratio_before_external_join = 0
);

-- Result equivalence across the gate settings, with `enable_parallel_replicas = 0`
-- so the coordinator step does not interfere with read-in-order matching (see the
-- note in `04209_top_k_through_join_read_in_order_gate.sql`).
SELECT 'result_swap_false' AS label, count(*), max(k), min(k) FROM (
    SELECT l.k AS k, r.value FROM t_l AS l LEFT JOIN t_r AS r ON r.k = l.k
    ORDER BY l.k DESC LIMIT 10
    SETTINGS query_plan_join_swap_table = false, enable_parallel_replicas = 0
);

SELECT 'result_swap_true' AS label, count(*), max(k), min(k) FROM (
    SELECT l.k AS k, r.value FROM t_l AS l LEFT JOIN t_r AS r ON r.k = l.k
    ORDER BY l.k DESC LIMIT 10
    SETTINGS query_plan_join_swap_table = true, enable_parallel_replicas = 0
);

DROP TABLE t_l;
DROP TABLE t_r;
