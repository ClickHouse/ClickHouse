-- Tags: no-parallel, no-random-settings, no-random-merge-tree-settings, no-parallel-replicas
-- no-parallel: Messes with internal cache.
-- no-random-* / no-parallel-replicas: Flaky check must not randomize settings or inject parallel replicas; breaks GROUP BY correctness and cache ProfileEvents.

-- `GROUPING SETS`: no pipeline hash; cold run has no plan-time hits; execution-time cache can still warm up.

SYSTEM DROP AGGREGATE CACHE;

DROP TABLE IF EXISTS test_partial_agg_gs_analyzer;

CREATE TABLE test_partial_agg_gs_analyzer (
    date Date,
    category String,
    value Int64
) ENGINE = MergeTree()
ORDER BY (date, category);

SYSTEM STOP MERGES test_partial_agg_gs_analyzer;

SET allow_experimental_analyzer = 1;
SET optimize_aggregation_in_order = 0;
SET max_rows_to_group_by = 0;
SET group_by_overflow_mode = 'throw';

INSERT INTO test_partial_agg_gs_analyzer SELECT '2024-01-01', 'A', number FROM numbers(10000);
INSERT INTO test_partial_agg_gs_analyzer SELECT '2024-01-01', 'B', number FROM numbers(10000);

SELECT '--- First run (cold cache, no plan-time pipeline hash for grouping sets)';

SELECT
    grouping(category),
    if(toUInt8(grouping(category)), '<total>', category) AS bucket,
    sum(value),
    count()
FROM test_partial_agg_gs_analyzer
GROUP BY GROUPING SETS ((category), ())
ORDER BY grouping(category), bucket
SETTINGS
    allow_experimental_analyzer = 1,
    use_partial_aggregate_cache = 1,
    optimize_aggregation_in_order = 0,
    max_rows_to_group_by = 0,
    group_by_overflow_mode = 'throw',
    log_comment = 'test_partial_agg_gs_analyzer_q1';

SYSTEM FLUSH LOGS query_log;

SELECT
    (ProfileEvents['PartialAggregateCacheHits'] = 0) AND (ProfileEvents['PartialAggregateCacheMisses'] > 0) AS cold_ok
FROM system.query_log
WHERE
    type = 'QueryFinish'
    AND current_database = currentDatabase()
    AND log_comment = 'test_partial_agg_gs_analyzer_q1'
    AND is_initial_query = 1
ORDER BY event_time_microseconds DESC
LIMIT 1;

SELECT '--- Second run (warm execution-time cache)';

SELECT
    grouping(category),
    if(toUInt8(grouping(category)), '<total>', category) AS bucket,
    sum(value),
    count()
FROM test_partial_agg_gs_analyzer
GROUP BY GROUPING SETS ((category), ())
ORDER BY grouping(category), bucket
SETTINGS
    allow_experimental_analyzer = 1,
    use_partial_aggregate_cache = 1,
    optimize_aggregation_in_order = 0,
    max_rows_to_group_by = 0,
    group_by_overflow_mode = 'throw',
    log_comment = 'test_partial_agg_gs_analyzer_q2';

SYSTEM FLUSH LOGS query_log;

SELECT ProfileEvents['PartialAggregateCacheHits'] > 0 AS warm_hits
FROM system.query_log
WHERE
    type = 'QueryFinish'
    AND current_database = currentDatabase()
    AND log_comment = 'test_partial_agg_gs_analyzer_q2'
    AND is_initial_query = 1
ORDER BY event_time_microseconds DESC
LIMIT 1;

DROP TABLE test_partial_agg_gs_analyzer;
