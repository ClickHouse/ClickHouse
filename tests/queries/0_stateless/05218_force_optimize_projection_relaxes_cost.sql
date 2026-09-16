SET enable_analyzer = 1;
SET enable_parallel_replicas = 0, automatic_parallel_replicas_mode = 0;
SET optimize_use_projections = 1, optimize_use_implicit_projections = 1, optimize_trivial_count_query = 0, optimize_aggregation_in_order = 0, use_query_condition_cache = 0;
SET optimize_move_to_prewhere = 1, query_plan_optimize_prewhere = 1, query_plan_remove_unused_columns = 1;

DROP TABLE IF EXISTS t_relax_normal;
DROP TABLE IF EXISTS t_relax_agg;

CREATE TABLE t_relax_normal
(
    key UInt64,
    value UInt64,
    other UInt64,
    PROJECTION p_other (SELECT key, value, other ORDER BY other)
)
ENGINE = MergeTree ORDER BY key
SETTINGS index_granularity = 8192, index_granularity_bytes = 10485760, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

CREATE TABLE t_relax_agg
(
    key UInt64,
    other UInt64,
    PROJECTION p_count (SELECT key, count() GROUP BY key)
)
ENGINE = MergeTree ORDER BY key
SETTINGS index_granularity = 8192, index_granularity_bytes = 10485760, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t_relax_normal SELECT number, number * 2, number % 100 FROM numbers(100000);
INSERT INTO t_relax_agg SELECT number, number % 100 FROM numbers(100000);

-- { echoOn }

-- The normal projection reads more marks than the base table, so it is rejected by cost and taken only when forced. The result is the same.
EXPLAIN indexes = 1, projections = 1 SELECT value FROM t_relax_normal WHERE key < 10;
EXPLAIN indexes = 1, projections = 1 SELECT value FROM t_relax_normal WHERE key < 10 SETTINGS force_optimize_projection = 1;
SELECT sum(value) FROM t_relax_normal WHERE key < 10;
SELECT sum(value) FROM t_relax_normal WHERE key < 10 SETTINGS force_optimize_projection = 1;

-- The aggregate projection reads more marks than the implicit one, so it is taken only when forced with the implicit projection off. The result is the same.
EXPLAIN indexes = 1, projections = 1 SELECT count() FROM t_relax_agg;
EXPLAIN indexes = 1, projections = 1 SELECT count() FROM t_relax_agg SETTINGS force_optimize_projection = 1, optimize_use_implicit_projections = 0;
SELECT count() FROM t_relax_agg;
SELECT count() FROM t_relax_agg SETTINGS force_optimize_projection = 1, optimize_use_implicit_projections = 0;
