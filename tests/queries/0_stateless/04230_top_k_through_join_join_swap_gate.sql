-- `topKThroughJoin` defers to second-pass read-in-order only when the join side
-- is guaranteed stable (an allowed `LEFT`->`RIGHT` swap would make pass 2 reject it).

SET enable_analyzer = 1;
SET query_plan_top_k_through_join = 1;

DROP TABLE IF EXISTS t_l;
DROP TABLE IF EXISTS t_r;

CREATE TABLE t_l (k Int64, payload String) ENGINE = MergeTree() ORDER BY k;
CREATE TABLE t_r (k Int64, value String) ENGINE = MergeTree() ORDER BY k;

INSERT INTO t_l SELECT number, repeat('a', 8) FROM numbers(1000);
INSERT INTO t_r SELECT number, repeat('b', 8) FROM numbers(1000);

-- `swap_table = false`: deferral fires, outer Sort + Limit satisfied via read-in-order. One of each.
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

-- `swap_table = true`: gate makes `topKThroughJoin` inject its own inner Sort + Limit. Two of each.
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
