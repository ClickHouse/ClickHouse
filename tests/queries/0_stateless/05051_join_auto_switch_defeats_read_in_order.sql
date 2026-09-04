-- `join_algorithm = 'auto'` uses `JoinSwitcher`, which can drain onto
-- `PartialMergeJoin` and re-sort left blocks. `preservesLeftBlockOrder()` is
-- therefore false at plan time, and `topKThroughJoin` must inject its own
-- `Sort + Limit` instead of deferring to read-in-order through the join.
-- See issue 110662.
-- Random settings limits: max_bytes_before_external_join=(0, 0); max_bytes_ratio_before_external_join=(0, 0); max_rows_in_join=(50, 50); max_bytes_in_join=(0, 0); enable_analyzer=(1, 1); query_plan_top_k_through_join=(1, 1)

SET enable_analyzer = 1;
SET query_plan_top_k_through_join = 1;
SET query_plan_optimize_join_order_randomize = 0;

DROP TABLE IF EXISTS t05051_l;
DROP TABLE IF EXISTS t05051_r;

CREATE TABLE t05051_l (k Int64, payload String) ENGINE = MergeTree ORDER BY k;
CREATE TABLE t05051_r (k Int64, value String) ENGINE = MergeTree ORDER BY k;

INSERT INTO t05051_l SELECT number, repeat('a', 8) FROM numbers(1000);
INSERT INTO t05051_r SELECT number, repeat('b', 8) FROM numbers(1000);

-- Control: plain `hash` still defers (one outer Sort + Limit).
SELECT 'hash' AS label, countIf(explain LIKE '%Sorting%') AS sort_count, countIf(explain LIKE '%Limit%') AS limit_count
FROM ( EXPLAIN actions = 0
    SELECT l.k, r.value FROM t05051_l AS l LEFT JOIN t05051_r AS r ON r.k = l.k
    ORDER BY l.k DESC LIMIT 10
    SETTINGS optimize_read_in_order = 1,
             query_plan_read_in_order = 1, query_plan_read_in_order_through_join = 1,
             query_plan_join_swap_table = false, query_plan_max_limit_for_top_k_optimization = 0,
             enable_join_runtime_filters = 0, enable_lazy_columns_replication = 0,
             query_plan_optimize_lazy_materialization = 0,
             enable_parallel_replicas = 0, enable_analyzer = 1,
             join_algorithm = 'hash',
             max_bytes_before_external_join = 0, max_bytes_ratio_before_external_join = 0
);

-- `auto` with a small `max_rows_in_join` so a drain is possible. Extra Sort + Limit.
SELECT 'auto' AS label, countIf(explain LIKE '%Sorting%') AS sort_count, countIf(explain LIKE '%Limit%') AS limit_count
FROM ( EXPLAIN actions = 0
    SELECT l.k, r.value FROM t05051_l AS l LEFT JOIN t05051_r AS r ON r.k = l.k
    ORDER BY l.k DESC LIMIT 10
    SETTINGS optimize_read_in_order = 1,
             query_plan_read_in_order = 1, query_plan_read_in_order_through_join = 1,
             query_plan_join_swap_table = false, query_plan_max_limit_for_top_k_optimization = 0,
             enable_join_runtime_filters = 0, enable_lazy_columns_replication = 0,
             query_plan_optimize_lazy_materialization = 0,
             enable_parallel_replicas = 0, enable_analyzer = 1,
             join_algorithm = 'auto',
             max_rows_in_join = 50, max_bytes_in_join = 0,
             max_bytes_before_external_join = 0, max_bytes_ratio_before_external_join = 0
);

DROP TABLE t05051_l;
DROP TABLE t05051_r;
