-- Verify that `topKThroughJoin`'s deferral probe mirrors
-- `ReadFromMerge::requestReadingInOrder`, which rejects reverse order with `FINAL`.
--
-- Same gate as `04234_top_k_through_join_final_desc_gate`, but the preserved side reads
-- from a `Merge` table over `ReplacingMergeTree` tables, and the join is spill-capable
-- (an absolute `max_bytes_before_external_join` wraps it in `SpillingHashJoin`). If the
-- probe returned `true` here, `topKThroughJoin` would step aside, pass 2 would still
-- reject the descending `FINAL` read, and the query would lose both optimizations.

SET enable_analyzer = 1;
SET query_plan_top_k_through_join = 1, query_plan_max_limit_for_top_k_optimization = 1000;

DROP TABLE IF EXISTS t_merge_final_l_1;
DROP TABLE IF EXISTS t_merge_final_l_2;
DROP TABLE IF EXISTS t_merge_final_l;
DROP TABLE IF EXISTS t_merge_final_r;

CREATE TABLE t_merge_final_l_1 (k Int64, payload String) ENGINE = ReplacingMergeTree() ORDER BY k;
CREATE TABLE t_merge_final_l_2 (k Int64, payload String) ENGINE = ReplacingMergeTree() ORDER BY k;
CREATE TABLE t_merge_final_l (k Int64, payload String)
ENGINE = Merge(currentDatabase(), '^t_merge_final_l_[12]$');
CREATE TABLE t_merge_final_r (k Int64, value String) ENGINE = MergeTree() ORDER BY k;

INSERT INTO t_merge_final_l_1 SELECT number, repeat('a', 8) FROM numbers(500);
INSERT INTO t_merge_final_l_2 SELECT number + 500, repeat('a', 8) FROM numbers(500);
INSERT INTO t_merge_final_r SELECT number, repeat('b', 8) FROM numbers(1000);

-- FINAL + DESC: pass 2's `ReadFromMerge::requestReadingInOrder` rejects descending
-- direction with FINAL, so `topKThroughJoin` must NOT defer. Expect two Sort + Limit
-- pairs in the plan (the outer pair + the injected pair on the preserved input).
SELECT 'merge_final_desc' AS label, countIf(explain LIKE '%Sorting%') AS sort_count, countIf(explain LIKE '%Limit%') AS limit_count
FROM ( EXPLAIN actions = 0
    SELECT l.k, r.value FROM t_merge_final_l AS l FINAL LEFT JOIN t_merge_final_r AS r ON r.k = l.k
    ORDER BY l.k DESC LIMIT 10
    SETTINGS optimize_read_in_order = 1,
             query_plan_read_in_order = 1, query_plan_read_in_order_through_join = 1,
             query_plan_read_in_order_through_spilling_join = 1,
             query_plan_join_swap_table = false,
             enable_join_runtime_filters = 0, enable_lazy_columns_replication = 0,
             query_plan_optimize_lazy_materialization = 0,
             enable_parallel_replicas = 0, join_algorithm = 'hash',
             max_bytes_before_external_join = 1, max_bytes_ratio_before_external_join = 0
);

-- FINAL + ASC: pass 2 accepts ascending direction with FINAL, so the deferral is sound
-- and `topKThroughJoin` should NOT inject its own `Sort + Limit` - only the outer pair
-- remains, with the spill-capable join pinned in memory by pass 2.
SELECT 'merge_final_asc' AS label, countIf(explain LIKE '%Sorting%') AS sort_count, countIf(explain LIKE '%Limit%') AS limit_count
FROM ( EXPLAIN actions = 0
    SELECT l.k, r.value FROM t_merge_final_l AS l FINAL LEFT JOIN t_merge_final_r AS r ON r.k = l.k
    ORDER BY l.k ASC LIMIT 10
    SETTINGS optimize_read_in_order = 1,
             query_plan_read_in_order = 1, query_plan_read_in_order_through_join = 1,
             query_plan_read_in_order_through_spilling_join = 1,
             query_plan_join_swap_table = false,
             enable_join_runtime_filters = 0, enable_lazy_columns_replication = 0,
             query_plan_optimize_lazy_materialization = 0,
             enable_parallel_replicas = 0, join_algorithm = 'hash',
             max_bytes_before_external_join = 1, max_bytes_ratio_before_external_join = 0
);

-- Result equivalence across orderings (no spill threshold here - correctness of the
-- ordering is what matters, and the plan shapes are pinned above).
SELECT 'result_merge_final_desc' AS label, count(*), max(k), min(k) FROM (
    SELECT l.k AS k, r.value FROM t_merge_final_l AS l FINAL LEFT JOIN t_merge_final_r AS r ON r.k = l.k
    ORDER BY l.k DESC LIMIT 10
    SETTINGS query_plan_read_in_order = 1, query_plan_read_in_order_through_join = 1,
             enable_parallel_replicas = 0,
             max_bytes_before_external_join = 0, max_bytes_ratio_before_external_join = 0
);

SELECT 'result_merge_final_asc' AS label, count(*), max(k), min(k) FROM (
    SELECT l.k AS k, r.value FROM t_merge_final_l AS l FINAL LEFT JOIN t_merge_final_r AS r ON r.k = l.k
    ORDER BY l.k ASC LIMIT 10
    SETTINGS query_plan_read_in_order = 1, query_plan_read_in_order_through_join = 1,
             enable_parallel_replicas = 0,
             max_bytes_before_external_join = 0, max_bytes_ratio_before_external_join = 0
);

DROP TABLE t_merge_final_l;
DROP TABLE t_merge_final_l_1;
DROP TABLE t_merge_final_l_2;
DROP TABLE t_merge_final_r;
