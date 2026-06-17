-- `topKThroughJoin` must not defer to read-in-order for `FINAL` + descending order:
-- `requestReadingInOrder` rejects descending direction under FINAL, so the deferral
-- would lose both optimizations. The gate makes it inject its own inner Sort + Limit.

SET enable_analyzer = 1;
SET query_plan_top_k_through_join = 1;

DROP TABLE IF EXISTS t_l_final;
DROP TABLE IF EXISTS t_r_final;

CREATE TABLE t_l_final (k Int64, payload String) ENGINE = ReplacingMergeTree() ORDER BY k;
CREATE TABLE t_r_final (k Int64, value String) ENGINE = MergeTree() ORDER BY k;

INSERT INTO t_l_final SELECT number, repeat('a', 8) FROM numbers(1000);
INSERT INTO t_r_final SELECT number, repeat('b', 8) FROM numbers(1000);

-- FINAL + DESC: no deferral, two Sort + Limit pairs (outer + injected).
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

-- FINAL + ASC: deferral is sound, only the outer Sort + Limit pair remains.
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
