SET enable_analyzer = 1;
SET enable_parallel_replicas = 0, automatic_parallel_replicas_mode = 0;
SET optimize_use_projections = 1, use_query_condition_cache = 0;
SET optimize_move_to_prewhere = 1, query_plan_optimize_prewhere = 1, query_plan_remove_unused_columns = 1;

DROP TABLE IF EXISTS t_normal;

CREATE TABLE t_normal
(
    key UInt64,
    value UInt64,
    other UInt64,
    PROJECTION p_other (SELECT key, value, other ORDER BY other),
    PROJECTION p_value (SELECT key, value ORDER BY value)
)
ENGINE = MergeTree ORDER BY key
SETTINGS index_granularity = 8192, index_granularity_bytes = 10485760, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t_normal SELECT number, number * 2, number % 100 FROM numbers(100000);

-- { echoOn }

-- The projection reads more marks than the base table, so it is used only when forced. The result is the same.
EXPLAIN indexes = 1, projections = 1 SELECT value FROM t_normal WHERE key < 10;
EXPLAIN indexes = 1, projections = 1 SELECT value FROM t_normal PROJECTION p_other WHERE key < 10;
SELECT sum(value) FROM t_normal WHERE key < 10;
SELECT sum(value) FROM t_normal PROJECTION p_other WHERE key < 10;

-- The modifier wins over the preferred projection setting.
EXPLAIN indexes = 1, projections = 1 SELECT value FROM t_normal PROJECTION p_other WHERE key < 10 SETTINGS preferred_optimize_projection_name = 'p_value';

-- The force settings see the projection as used.
SELECT sum(value) FROM t_normal PROJECTION p_other WHERE key < 10 SETTINGS force_optimize_projection = 1, force_optimize_projection_name = 'p_other';

-- Only the first optimization pass depends on this setting, the projection is still applied.
SELECT sum(value) FROM t_normal PROJECTION p_other WHERE key < 10 SETTINGS query_plan_enable_optimizations = 0;
