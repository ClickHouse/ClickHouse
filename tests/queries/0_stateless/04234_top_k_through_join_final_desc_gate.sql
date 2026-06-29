-- Verify that `topKThroughJoin`'s deferral to `optimizeReadInOrder` does not
-- silently disable itself for `FINAL` + descending order.
--
-- `wouldReadInOrderBeUseful` matches the storage's sorting key against the
-- requested sort description but is unaware of FINAL-time gating in
-- `ReadFromMergeTree::requestReadingInOrder`, which rejects
-- `direction != 1 && query_info.isFinal()`. Without the FINAL+descending gate
-- in `topKThroughJoin`, the deferral would fire, the second pass would still
-- reject the read, and both optimizations would be lost. This test pins the
-- relevant settings on and checks that an inner `Sort + Limit` is injected.

SET enable_analyzer = 1;
SET query_plan_top_k_through_join = 1;

DROP TABLE IF EXISTS t_l_final;
DROP TABLE IF EXISTS t_r_final;

CREATE TABLE t_l_final (k Int64, payload String) ENGINE = ReplacingMergeTree() ORDER BY k;
CREATE TABLE t_r_final (k Int64, value String) ENGINE = MergeTree() ORDER BY k;

INSERT INTO t_l_final SELECT number, repeat('a', 8) FROM numbers(1000);
INSERT INTO t_r_final SELECT number, repeat('b', 8) FROM numbers(1000);

-- FINAL + DESC: pass 2's `requestReadingInOrder` rejects descending direction
-- with FINAL, so `topKThroughJoin` must NOT defer. Expect two Sort + Limit
-- pairs in the plan (the outer pair + the injected pair on the preserved input).
SELECT 'final_desc' AS label, countIf(explain LIKE '%Sorting%') AS sort_count, countIf(explain LIKE '%Limit%') AS limit_count
FROM ( EXPLAIN actions = 0
    SELECT l.k, r.value FROM t_l_final AS l FINAL LEFT JOIN t_r_final AS r ON r.k = l.k
    ORDER BY l.k DESC LIMIT 10
    SETTINGS optimize_read_in_order = 1,
             query_plan_read_in_order = 1, query_plan_read_in_order_through_join = 1,
             query_plan_join_swap_table = false, query_plan_max_limit_for_top_k_optimization = 0,
             enable_join_runtime_filters = 0, enable_lazy_columns_replication = 0,
             query_plan_optimize_lazy_materialization = 0,
             enable_parallel_replicas = 0,
             max_bytes_before_external_join = 0, max_bytes_ratio_before_external_join = 0
);

-- FINAL + ASC: pass 2 accepts ascending direction with FINAL, so the deferral
-- is sound and `topKThroughJoin` should NOT inject its own `Sort + Limit` -
-- only the outer pair remains.
SELECT 'final_asc' AS label, countIf(explain LIKE '%Sorting%') AS sort_count, countIf(explain LIKE '%Limit%') AS limit_count
FROM ( EXPLAIN actions = 0
    SELECT l.k, r.value FROM t_l_final AS l FINAL LEFT JOIN t_r_final AS r ON r.k = l.k
    ORDER BY l.k ASC LIMIT 10
    SETTINGS optimize_read_in_order = 1,
             query_plan_read_in_order = 1, query_plan_read_in_order_through_join = 1,
             query_plan_join_swap_table = false, query_plan_max_limit_for_top_k_optimization = 0,
             enable_join_runtime_filters = 0, enable_lazy_columns_replication = 0,
             query_plan_optimize_lazy_materialization = 0,
             enable_parallel_replicas = 0,
             max_bytes_before_external_join = 0, max_bytes_ratio_before_external_join = 0
);

-- Result equivalence across orderings. `enable_parallel_replicas = 0` for the
-- same coordination-mode reason as in `04209_top_k_through_join_read_in_order_gate`.
SELECT 'result_final_desc' AS label, count(*), max(k), min(k) FROM (
    SELECT l.k AS k, r.value FROM t_l_final AS l FINAL LEFT JOIN t_r_final AS r ON r.k = l.k
    ORDER BY l.k DESC LIMIT 10
    SETTINGS query_plan_read_in_order = 1, query_plan_read_in_order_through_join = 1,
             enable_parallel_replicas = 0
);

SELECT 'result_final_asc' AS label, count(*), max(k), min(k) FROM (
    SELECT l.k AS k, r.value FROM t_l_final AS l FINAL LEFT JOIN t_r_final AS r ON r.k = l.k
    ORDER BY l.k ASC LIMIT 10
    SETTINGS query_plan_read_in_order = 1, query_plan_read_in_order_through_join = 1,
             enable_parallel_replicas = 0
);

DROP TABLE t_l_final;
DROP TABLE t_r_final;
