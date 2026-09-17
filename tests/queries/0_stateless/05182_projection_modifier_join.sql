SET enable_analyzer = 1;
SET enable_parallel_replicas = 0, automatic_parallel_replicas_mode = 0;
SET optimize_use_projections = 1, optimize_aggregation_in_order = 0, use_query_condition_cache = 0;
SET optimize_move_to_prewhere = 1, query_plan_optimize_prewhere = 1, query_plan_remove_unused_columns = 1;
SET query_plan_optimize_join_order_randomize = 0, query_plan_optimize_join_order_limit = 0, query_plan_join_swap_table = 'false', enable_join_runtime_filters = 0;

DROP TABLE IF EXISTS t_join_normal;
DROP TABLE IF EXISTS t_join_agg;

CREATE TABLE t_join_normal
(
    key UInt64,
    value UInt64,
    other UInt64,
    PROJECTION p_other (SELECT key, value, other ORDER BY other),
    PROJECTION p_value (SELECT key, value ORDER BY value)
)
ENGINE = MergeTree ORDER BY key
SETTINGS index_granularity = 8192, index_granularity_bytes = 10485760, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t_join_normal SELECT number, number * 2, number % 100 FROM numbers(100000);

CREATE TABLE t_join_agg
(
    key UInt64,
    other UInt64,
    PROJECTION p_other_key (SELECT other, key, count() GROUP BY other, key)
)
ENGINE = MergeTree ORDER BY key
SETTINGS index_granularity = 8192, index_granularity_bytes = 10485760, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t_join_agg SELECT number, number % 100 FROM numbers(100000);

-- { echoOn }

-- Each side of a join carries its own projection.
EXPLAIN indexes = 1, projections = 1
SELECT count()
FROM (SELECT key FROM t_join_normal PROJECTION p_other WHERE key < 10) AS a
INNER JOIN (SELECT key, count() AS c FROM t_join_agg PROJECTION p_other_key WHERE key < 10 GROUP BY key) AS b USING (key);

SELECT count()
FROM (SELECT key FROM t_join_normal PROJECTION p_other WHERE key < 10) AS a
INNER JOIN (SELECT key, count() AS c FROM t_join_agg PROJECTION p_other_key WHERE key < 10 GROUP BY key) AS b USING (key)
SETTINGS force_optimize_projection_name = 'p_other_key';

-- A self join reads the same table through two different projections.
EXPLAIN indexes = 1, projections = 1
SELECT count()
FROM t_join_normal AS a PROJECTION p_other
INNER JOIN t_join_normal AS b PROJECTION p_value ON a.key = b.key
WHERE a.other < 5 AND b.value < 100;

SELECT count()
FROM t_join_normal AS a PROJECTION p_other
INNER JOIN t_join_normal AS b PROJECTION p_value ON a.key = b.key
WHERE a.other < 5 AND b.value < 100
SETTINGS log_comment = '05182_projection_modifier_self_join';

SYSTEM FLUSH LOGS query_log;
SELECT arraySort(projections) FROM system.query_log
WHERE current_database = currentDatabase() AND log_comment = '05182_projection_modifier_self_join' AND type = 'QueryFinish';
