-- Verify `topKThroughJoin` fires for `RIGHT JOIN` even when the second-pass
-- `optimizeReadInOrder` cannot pick it up (it only traverses INNER/LEFT joins).

SET enable_analyzer = 1;
SET query_plan_top_k_through_join = 1;

DROP TABLE IF EXISTS t_l;
DROP TABLE IF EXISTS t_r;

CREATE TABLE t_l (k Int64, payload String) ENGINE = MergeTree() ORDER BY k;
CREATE TABLE t_r (k Int64, value String) ENGINE = MergeTree() ORDER BY k;

INSERT INTO t_l SELECT number, repeat('a', 8) FROM numbers(1000);
INSERT INTO t_r SELECT number, repeat('b', 8) FROM numbers(1000);

-- The deferral is gated on `LEFT`, so for `RIGHT JOIN` topKThroughJoin fires and
-- adds an inner Sort + Limit on the right preserved input (count=2).
SELECT 'right_join_topk' AS label, countIf(explain LIKE '%Sorting%') AS sort_count, countIf(explain LIKE '%Limit%') AS limit_count
FROM ( EXPLAIN actions = 0
    SELECT l.k, r.value FROM t_l AS l RIGHT JOIN t_r AS r ON r.k = l.k
    ORDER BY r.k DESC LIMIT 10
    SETTINGS optimize_read_in_order = 1,
             query_plan_read_in_order = 1, query_plan_read_in_order_through_join = 1,
             query_plan_join_swap_table = false, query_plan_max_limit_for_top_k_optimization = 0,
             enable_join_runtime_filters = 0, enable_lazy_columns_replication = 0,
             query_plan_optimize_lazy_materialization = 0,
             enable_parallel_replicas = 0
);

-- Result equivalence: top 10 by `r.k DESC` over identical 1..1000 ranges are 999..990.
SELECT 'right_join_result' AS label, count(*), max(rk), min(rk) FROM (
    SELECT r.k AS rk, r.value FROM t_l AS l RIGHT JOIN t_r AS r ON r.k = l.k
    ORDER BY r.k DESC LIMIT 10
    SETTINGS enable_parallel_replicas = 0
);

DROP TABLE t_l;
DROP TABLE t_r;
