SET enable_analyzer = 1;
SET enable_parallel_replicas = 0, automatic_parallel_replicas_mode = 0;
SET optimize_use_projections = 1, optimize_use_implicit_projections = 1, optimize_trivial_count_query = 1, optimize_aggregation_in_order = 0, use_query_condition_cache = 0;
SET optimize_move_to_prewhere = 1, query_plan_optimize_prewhere = 1, query_plan_remove_unused_columns = 1;

DROP TABLE IF EXISTS t_agg;

CREATE TABLE t_agg
(
    key UInt64,
    other UInt64,
    PROJECTION p_count (SELECT key, count() GROUP BY key),
    PROJECTION p_other_key (SELECT other, key, count() GROUP BY other, key)
)
ENGINE = MergeTree ORDER BY key
SETTINGS index_granularity = 8192, index_granularity_bytes = 10485760, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t_agg SELECT number, number % 100 FROM numbers(100000);

-- { echoOn }

-- count() goes through the named projection instead of the trivial count or the implicit projection.
EXPLAIN indexes = 1, projections = 1 SELECT count() FROM t_agg;
EXPLAIN indexes = 1, projections = 1 SELECT count() FROM t_agg PROJECTION p_count;
SELECT count() FROM t_agg PROJECTION p_count;

-- The projection reads more marks than the base table, so it is used only when forced. The result is the same.
EXPLAIN indexes = 1, projections = 1 SELECT other, count() FROM t_agg WHERE key < 10 GROUP BY other ORDER BY other;
EXPLAIN indexes = 1, projections = 1 SELECT other, count() FROM t_agg PROJECTION p_other_key WHERE key < 10 GROUP BY other ORDER BY other;
SELECT other, count() FROM t_agg WHERE key < 10 GROUP BY other ORDER BY other;
SELECT other, count() FROM t_agg PROJECTION p_other_key WHERE key < 10 GROUP BY other ORDER BY other;
