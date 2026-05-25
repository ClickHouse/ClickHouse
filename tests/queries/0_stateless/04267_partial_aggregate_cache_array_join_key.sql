-- Tags: no-parallel, no-random-settings, no-random-merge-tree-settings, no-parallel-replicas
-- no-parallel: Messes with internal cache.
-- no-random-* / no-parallel-replicas: Flaky check must not randomize settings or inject parallel replicas.

-- Partial aggregate cache: `ARRAY JOIN` must be part of semantic key.

SYSTEM DROP AGGREGATE CACHE;

DROP TABLE IF EXISTS test_partial_agg_cache_array_join_key;

CREATE TABLE test_partial_agg_cache_array_join_key
(
    id UInt32,
    arr Array(UInt32),
    value Int64
)
ENGINE = MergeTree()
ORDER BY id;

SYSTEM STOP MERGES test_partial_agg_cache_array_join_key;

SET optimize_aggregation_in_order = 0;
SET max_rows_to_group_by = 0;
SET group_by_overflow_mode = 'throw';

INSERT INTO test_partial_agg_cache_array_join_key VALUES
    (1, [10, 11], 10),
    (2, [20], 20);

SELECT '--- Warm cache without ARRAY JOIN';

SELECT sum(value)
FROM test_partial_agg_cache_array_join_key
SETTINGS
    use_partial_aggregate_cache = 1,
    optimize_aggregation_in_order = 0,
    max_rows_to_group_by = 0,
    group_by_overflow_mode = 'throw',
    log_comment = 'test_partial_agg_cache_array_join_key_warm';

SYSTEM FLUSH LOGS query_log;

SELECT
    ProfileEvents['PartialAggregateCacheHits'] = 0 AS no_hits,
    ProfileEvents['PartialAggregateCacheMisses'] > 0 AS has_misses
FROM system.query_log
WHERE
    type = 'QueryFinish'
    AND current_database = currentDatabase()
    AND log_comment = 'test_partial_agg_cache_array_join_key_warm'
    AND is_initial_query = 1
ORDER BY event_time_microseconds DESC
LIMIT 1;

SELECT '--- ARRAY JOIN query must not reuse previous states';

SELECT sum(value)
FROM test_partial_agg_cache_array_join_key
ARRAY JOIN arr
SETTINGS
    use_partial_aggregate_cache = 1,
    optimize_aggregation_in_order = 0,
    max_rows_to_group_by = 0,
    group_by_overflow_mode = 'throw',
    log_comment = 'test_partial_agg_cache_array_join_key_array_join';

SYSTEM FLUSH LOGS query_log;

SELECT
    ProfileEvents['PartialAggregateCacheHits'] = 0 AS no_hits,
    ProfileEvents['PartialAggregateCacheMisses'] > 0 AS has_misses
FROM system.query_log
WHERE
    type = 'QueryFinish'
    AND current_database = currentDatabase()
    AND log_comment = 'test_partial_agg_cache_array_join_key_array_join'
    AND is_initial_query = 1
ORDER BY event_time_microseconds DESC
LIMIT 1;

DROP TABLE test_partial_agg_cache_array_join_key;
