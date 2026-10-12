-- Tags: no-parallel, no-random-settings, no-random-merge-tree-settings, no-parallel-replicas
-- no-parallel: Messes with internal cache.
-- no-random-* / no-parallel-replicas: Flaky check must not randomize settings or inject parallel replicas; breaks GROUP BY correctness and cache ProfileEvents.

-- MergeTree + aggregate projection + `use_partial_aggregate_cache`. Assert result only; ProfileEvents for cache vary by build.

SYSTEM DROP AGGREGATE CACHE;

DROP TABLE IF EXISTS test_partial_agg_proj;

CREATE TABLE test_partial_agg_proj (
    category String,
    value Int64,
    PROJECTION p_agg (SELECT category, sum(value) GROUP BY category)
) ENGINE = MergeTree()
ORDER BY category;

SYSTEM STOP MERGES test_partial_agg_proj;

SET optimize_aggregation_in_order = 0;
SET max_rows_to_group_by = 0;
SET group_by_overflow_mode = 'throw';

INSERT INTO test_partial_agg_proj SELECT 'A', number FROM numbers(10000);
INSERT INTO test_partial_agg_proj SELECT 'B', number FROM numbers(10000);

SELECT '--- Aggregate projection table: first run';

SELECT category, sum(value)
FROM test_partial_agg_proj
GROUP BY category
ORDER BY category
SETTINGS
    use_partial_aggregate_cache = 1,
    optimize_use_projections = 1,
    force_optimize_projection = 1,
    force_optimize_projection_name = 'p_agg',
    optimize_aggregation_in_order = 0,
    max_rows_to_group_by = 0,
    group_by_overflow_mode = 'throw',
    log_comment = 'test_partial_agg_proj_q1';

SELECT '--- Aggregate projection table: second run';

SELECT category, sum(value)
FROM test_partial_agg_proj
GROUP BY category
ORDER BY category
SETTINGS
    use_partial_aggregate_cache = 1,
    optimize_use_projections = 1,
    force_optimize_projection = 1,
    force_optimize_projection_name = 'p_agg',
    optimize_aggregation_in_order = 0,
    max_rows_to_group_by = 0,
    group_by_overflow_mode = 'throw',
    log_comment = 'test_partial_agg_proj_q2';

DROP TABLE test_partial_agg_proj;
