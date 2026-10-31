-- Tags: no-parallel, no-random-settings, no-random-merge-tree-settings, no-parallel-replicas
-- no-parallel: Messes with internal cache.
-- no-random-* / no-parallel-replicas: Flaky check must not randomize settings or inject parallel replicas; breaks GROUP BY correctness and cache ProfileEvents.

-- Partial aggregate cache: plan-time hit must survive pre-aggregation `FilterTransform`.
-- Use `WHERE` with `optimize_move_to_prewhere = 0` to force `FilterStep` before `AggregatingStep`.

SYSTEM DROP AGGREGATE CACHE;

DROP TABLE IF EXISTS test_partial_agg_filter_step;

SELECT '--- FilterStep: reference result without cache';

CREATE TABLE test_partial_agg_filter_step
(
    grp UInt8,
    value Int64
)
ENGINE = MergeTree()
ORDER BY (grp, value);

SYSTEM STOP MERGES test_partial_agg_filter_step;

SET optimize_aggregation_in_order = 0;
SET optimize_move_to_prewhere = 0;
SET max_rows_to_group_by = 0;
SET group_by_overflow_mode = 'throw';

INSERT INTO test_partial_agg_filter_step SELECT 1, number FROM numbers(10000);
INSERT INTO test_partial_agg_filter_step SELECT 2, number FROM numbers(10000);

SELECT grp, sum(value), count()
FROM test_partial_agg_filter_step
WHERE value % 3 != 1
GROUP BY grp
ORDER BY grp
SETTINGS
    use_partial_aggregate_cache = 0,
    optimize_aggregation_in_order = 0,
    optimize_move_to_prewhere = 0,
    max_rows_to_group_by = 0,
    group_by_overflow_mode = 'throw',
    log_comment = 'test_partial_agg_filter_step_ref';

SELECT '--- FilterStep: warm cache (expect misses)';

SELECT grp, sum(value), count()
FROM test_partial_agg_filter_step
WHERE value % 3 != 1
GROUP BY grp
ORDER BY grp
SETTINGS
    use_partial_aggregate_cache = 1,
    optimize_aggregation_in_order = 0,
    optimize_move_to_prewhere = 0,
    max_rows_to_group_by = 0,
    group_by_overflow_mode = 'throw',
    log_comment = 'test_partial_agg_filter_step_warm';

SYSTEM FLUSH LOGS query_log;

SELECT
    ProfileEvents['PartialAggregateCacheHits'] AS hits,
    ProfileEvents['PartialAggregateCacheMisses'] AS misses
FROM system.query_log
WHERE
    type = 'QueryFinish'
    AND current_database = currentDatabase()
    AND log_comment = 'test_partial_agg_filter_step_warm'
    AND is_initial_query = 1
ORDER BY event_time_microseconds DESC
LIMIT 1;

SELECT '--- FilterStep: second run (expect plan-time hits)';

SELECT grp, sum(value), count()
FROM test_partial_agg_filter_step
WHERE value % 3 != 1
GROUP BY grp
ORDER BY grp
SETTINGS
    use_partial_aggregate_cache = 1,
    optimize_aggregation_in_order = 0,
    optimize_move_to_prewhere = 0,
    max_rows_to_group_by = 0,
    group_by_overflow_mode = 'throw',
    log_comment = 'test_partial_agg_filter_step_hit';

SYSTEM FLUSH LOGS query_log;

SELECT
    ProfileEvents['PartialAggregateCacheHits'] AS hits,
    ProfileEvents['PartialAggregateCacheMisses'] AS misses
FROM system.query_log
WHERE
    type = 'QueryFinish'
    AND current_database = currentDatabase()
    AND log_comment = 'test_partial_agg_filter_step_hit'
    AND is_initial_query = 1
ORDER BY event_time_microseconds DESC
LIMIT 1;

DROP TABLE test_partial_agg_filter_step;
