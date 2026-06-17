-- Verify `topKThroughJoin` still fires when `query_plan_read_in_order_through_join`
-- is off (or spilling wraps the join), instead of deferring through both gates.

SET enable_analyzer = 1;
SET query_plan_top_k_through_join = 1;

DROP TABLE IF EXISTS t_l;
DROP TABLE IF EXISTS t_r;

-- Sort key (`k`) is the storage primary key, so `optimizeReadInOrder` could
-- stream rows in order; the deferral check decides whether to leave that to the
-- second pass or inject an explicit `Sort + Limit`.
CREATE TABLE t_l (k Int64, payload String) ENGINE = MergeTree() ORDER BY k;
CREATE TABLE t_r (k Int64, value String) ENGINE = MergeTree() ORDER BY k;

INSERT INTO t_l SELECT number, repeat('a', 8) FROM numbers(1000);
INSERT INTO t_r SELECT number, repeat('b', 8) FROM numbers(1000);

-- Both flags on: defer to `optimizeReadInOrder`, no explicit inner Sort + Limit.
SELECT 'both_on' AS label, countIf(explain LIKE '%Sorting%') AS sort_count, countIf(explain LIKE '%Limit%') AS limit_count
FROM ( EXPLAIN actions = 0
    SELECT l.k, r.value FROM t_l AS l LEFT JOIN t_r AS r ON r.k = l.k
    ORDER BY l.k DESC LIMIT 10
    SETTINGS optimize_read_in_order = 1,
             query_plan_read_in_order = 1, query_plan_read_in_order_through_join = 1,
             query_plan_join_swap_table = false, query_plan_max_limit_for_top_k_optimization = 0,
             enable_join_runtime_filters = 0, enable_lazy_columns_replication = 0,
             query_plan_optimize_lazy_materialization = 0,
             enable_parallel_replicas = 0,
             max_bytes_before_external_join = 0, max_bytes_ratio_before_external_join = 0
);

-- `read_in_order_through_join` off: the second pass cannot stream through the
-- join, so `topKThroughJoin` must fire (at least two of each step).
SELECT 'through_join_off' AS label, countIf(explain LIKE '%Sorting%') AS sort_count, countIf(explain LIKE '%Limit%') AS limit_count
FROM ( EXPLAIN actions = 0
    SELECT l.k, r.value FROM t_l AS l LEFT JOIN t_r AS r ON r.k = l.k
    ORDER BY l.k DESC LIMIT 10
    SETTINGS optimize_read_in_order = 1,
             query_plan_read_in_order = 1, query_plan_read_in_order_through_join = 0,
             query_plan_join_swap_table = false, query_plan_max_limit_for_top_k_optimization = 0,
             enable_join_runtime_filters = 0, enable_lazy_columns_replication = 0,
             query_plan_optimize_lazy_materialization = 0,
             enable_parallel_replicas = 0,
             max_bytes_before_external_join = 0, max_bytes_ratio_before_external_join = 0
);

-- Spilling-on path: `SpillingHashJoin` is rejected by the second-pass join
-- traversal, so the deferral must NOT fire and `topKThroughJoin` injects its own.
SELECT 'spilling_on' AS label, countIf(explain LIKE '%Sorting%') AS sort_count, countIf(explain LIKE '%Limit%') AS limit_count
FROM ( EXPLAIN actions = 0
    SELECT l.k, r.value FROM t_l AS l LEFT JOIN t_r AS r ON r.k = l.k
    ORDER BY l.k DESC LIMIT 10
    SETTINGS optimize_read_in_order = 1,
             query_plan_read_in_order = 1, query_plan_read_in_order_through_join = 1,
             query_plan_join_swap_table = false, query_plan_max_limit_for_top_k_optimization = 0,
             enable_join_runtime_filters = 0, enable_lazy_columns_replication = 0,
             query_plan_optimize_lazy_materialization = 0,
             enable_parallel_replicas = 0,
             max_bytes_ratio_before_external_join = 0.5
);

-- Result equivalence across flag combinations.
SELECT 'result_both_on' AS label, count(*), max(k), min(k) FROM (
    SELECT l.k AS k, r.value FROM t_l AS l LEFT JOIN t_r AS r ON r.k = l.k
    ORDER BY l.k DESC LIMIT 10
    SETTINGS query_plan_read_in_order = 1, query_plan_read_in_order_through_join = 1,
             enable_parallel_replicas = 0
);

SELECT 'result_through_join_off' AS label, count(*), max(k), min(k) FROM (
    SELECT l.k AS k, r.value FROM t_l AS l LEFT JOIN t_r AS r ON r.k = l.k
    ORDER BY l.k DESC LIMIT 10
    SETTINGS query_plan_read_in_order = 1, query_plan_read_in_order_through_join = 0,
             enable_parallel_replicas = 0
);

DROP TABLE t_l;
DROP TABLE t_r;
