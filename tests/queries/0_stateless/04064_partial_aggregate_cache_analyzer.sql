-- Tags: no-parallel, no-random-settings, no-random-merge-tree-settings, no-parallel-replicas
-- no-parallel: Messes with internal cache.
-- no-random-* / no-parallel-replicas: Flaky check must not randomize settings or inject parallel replicas; breaks GROUP BY correctness and cache ProfileEvents.

-- Regression: analyzer fills `partial_aggregate_cache_query_hash` for plan-time partial aggregate probe.

SYSTEM DROP AGGREGATE CACHE;

DROP TABLE IF EXISTS test_partial_agg_cache_analyzer;

CREATE TABLE test_partial_agg_cache_analyzer (
    date Date,
    category String,
    value Int64
) ENGINE = MergeTree()
ORDER BY (date, category);

SYSTEM STOP MERGES test_partial_agg_cache_analyzer;

SET allow_experimental_analyzer = 1;
SET optimize_aggregation_in_order = 0;
SET max_rows_to_group_by = 0;
SET group_by_overflow_mode = 'throw';

INSERT INTO test_partial_agg_cache_analyzer SELECT '2024-01-01', 'A', number FROM numbers(10000);
INSERT INTO test_partial_agg_cache_analyzer SELECT '2024-01-01', 'B', number FROM numbers(10000);

SELECT '--- First query (analyzer, cache miss expected)';

SELECT category, sum(value), count()
FROM test_partial_agg_cache_analyzer
GROUP BY category
ORDER BY category
SETTINGS
    allow_experimental_analyzer = 1,
    use_partial_aggregate_cache = 1,
    optimize_aggregation_in_order = 0,
    max_rows_to_group_by = 0,
    group_by_overflow_mode = 'throw',
    log_comment = 'test_partial_agg_cache_analyzer_q1';

SYSTEM FLUSH LOGS query_log;

SELECT
    ProfileEvents['PartialAggregateCacheHits'] AS hits,
    ProfileEvents['PartialAggregateCacheMisses'] AS misses
FROM system.query_log
WHERE
    type = 'QueryFinish'
    AND current_database = currentDatabase()
    AND log_comment = 'test_partial_agg_cache_analyzer_q1'
    AND is_initial_query = 1
ORDER BY event_time_microseconds DESC
LIMIT 1;

SELECT '--- Second query (analyzer, cache hit expected)';

SELECT category, sum(value), count()
FROM test_partial_agg_cache_analyzer
GROUP BY category
ORDER BY category
SETTINGS
    allow_experimental_analyzer = 1,
    use_partial_aggregate_cache = 1,
    optimize_aggregation_in_order = 0,
    max_rows_to_group_by = 0,
    group_by_overflow_mode = 'throw',
    log_comment = 'test_partial_agg_cache_analyzer_q2';

SYSTEM FLUSH LOGS query_log;

SELECT
    ProfileEvents['PartialAggregateCacheHits'] AS hits,
    ProfileEvents['PartialAggregateCacheMisses'] AS misses
FROM system.query_log
WHERE
    type = 'QueryFinish'
    AND current_database = currentDatabase()
    AND log_comment = 'test_partial_agg_cache_analyzer_q2'
    AND is_initial_query = 1
ORDER BY event_time_microseconds DESC
LIMIT 1;

DROP TABLE test_partial_agg_cache_analyzer;
