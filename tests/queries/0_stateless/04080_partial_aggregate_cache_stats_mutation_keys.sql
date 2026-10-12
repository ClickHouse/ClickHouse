-- Tags: no-parallel, no-random-settings, no-random-merge-tree-settings, no-parallel-replicas
-- no-parallel: Messes with internal cache.
-- no-random-* / no-parallel-replicas: Flaky check must not randomize settings or inject parallel replicas; breaks GROUP BY correctness and cache ProfileEvents.

-- Partial aggregate cache: `collect_hash_table_stats_during_aggregation`, mutation, multi-key, different WHERE.

SYSTEM DROP AGGREGATE CACHE;

DROP TABLE IF EXISTS test_partial_agg_cache;

CREATE TABLE test_partial_agg_cache (
    date Date,
    category String,
    value Int64
) ENGINE = MergeTree()
ORDER BY (date, category);

SYSTEM STOP MERGES test_partial_agg_cache;

SET optimize_aggregation_in_order = 0;
SET max_rows_to_group_by = 0;
SET group_by_overflow_mode = 'throw';

INSERT INTO test_partial_agg_cache SELECT '2024-01-01', 'A', number FROM numbers(10000);
INSERT INTO test_partial_agg_cache SELECT '2024-01-01', 'B', number FROM numbers(10000);
INSERT INTO test_partial_agg_cache SELECT '2024-01-02', 'A', number FROM numbers(10000);
INSERT INTO test_partial_agg_cache SELECT '2024-01-02', 'B', number FROM numbers(10000);
INSERT INTO test_partial_agg_cache SELECT '2024-01-03', 'A', number FROM numbers(10000);
INSERT INTO test_partial_agg_cache SELECT '2024-01-03', 'B', number FROM numbers(10000);

SELECT '--- collect_hash_table_stats_during_aggregation = 0: partial cache must still work';

SYSTEM DROP AGGREGATE CACHE;

SELECT category, sum(value), count()
FROM test_partial_agg_cache
GROUP BY category
ORDER BY category
SETTINGS
    use_partial_aggregate_cache = 1,
    collect_hash_table_stats_during_aggregation = 0,
    optimize_aggregation_in_order = 0,
    max_rows_to_group_by = 0,
    group_by_overflow_mode = 'throw',
    log_comment = 'test_partial_agg_cache_stats_off_q1';

SYSTEM FLUSH LOGS query_log;

SELECT
    ProfileEvents['PartialAggregateCacheHits'] AS hits,
    ProfileEvents['PartialAggregateCacheMisses'] AS misses
FROM system.query_log
WHERE
    type = 'QueryFinish'
    AND current_database = currentDatabase()
    AND log_comment = 'test_partial_agg_cache_stats_off_q1'
    AND is_initial_query = 1
ORDER BY event_time_microseconds DESC
LIMIT 1;

SELECT category, sum(value), count()
FROM test_partial_agg_cache
GROUP BY category
ORDER BY category
SETTINGS
    use_partial_aggregate_cache = 1,
    collect_hash_table_stats_during_aggregation = 0,
    optimize_aggregation_in_order = 0,
    max_rows_to_group_by = 0,
    group_by_overflow_mode = 'throw',
    log_comment = 'test_partial_agg_cache_stats_off_q2';

SYSTEM FLUSH LOGS query_log;

SELECT
    ProfileEvents['PartialAggregateCacheHits'] AS hits,
    ProfileEvents['PartialAggregateCacheMisses'] AS misses
FROM system.query_log
WHERE
    type = 'QueryFinish'
    AND current_database = currentDatabase()
    AND log_comment = 'test_partial_agg_cache_stats_off_q2'
    AND is_initial_query = 1
ORDER BY event_time_microseconds DESC
LIMIT 1;

SELECT '--- Mutation invalidation: mutated parts must miss';

SYSTEM DROP AGGREGATE CACHE;

SELECT category, sum(value), count()
FROM test_partial_agg_cache
GROUP BY category
ORDER BY category
SETTINGS
    use_partial_aggregate_cache = 1,
    optimize_aggregation_in_order = 0,
    max_rows_to_group_by = 0,
    group_by_overflow_mode = 'throw',
    log_comment = 'test_partial_agg_cache_mutation_warm';

SYSTEM START MERGES test_partial_agg_cache;
ALTER TABLE test_partial_agg_cache
    UPDATE value = value + 1
    WHERE date = '2024-01-01'
SETTINGS mutations_sync = 2;
SYSTEM STOP MERGES test_partial_agg_cache;

SELECT category, sum(value), count()
FROM test_partial_agg_cache
GROUP BY category
ORDER BY category
SETTINGS
    use_partial_aggregate_cache = 1,
    optimize_aggregation_in_order = 0,
    max_rows_to_group_by = 0,
    group_by_overflow_mode = 'throw',
    log_comment = 'test_partial_agg_cache_mutation_q1';

SYSTEM FLUSH LOGS query_log;

SELECT
    ProfileEvents['PartialAggregateCacheHits'] = 0 AS no_hits,
    ProfileEvents['PartialAggregateCacheMisses'] > 0 AS has_misses
FROM system.query_log
WHERE
    type = 'QueryFinish'
    AND current_database = currentDatabase()
    AND log_comment = 'test_partial_agg_cache_mutation_q1'
    AND is_initial_query = 1
ORDER BY event_time_microseconds DESC
LIMIT 1;

SELECT '--- Multi-key and different aggregate set (avg, uniqExact)';

SYSTEM DROP AGGREGATE CACHE;

SELECT date, category, avg(value), uniqExact(value)
FROM test_partial_agg_cache
GROUP BY date, category
ORDER BY date, category
SETTINGS
    use_partial_aggregate_cache = 1,
    optimize_aggregation_in_order = 0,
    max_rows_to_group_by = 0,
    group_by_overflow_mode = 'throw',
    log_comment = 'test_partial_agg_cache_multikey_q1';

SYSTEM FLUSH LOGS query_log;

SELECT
    ProfileEvents['PartialAggregateCacheHits'] = 0 AS no_hits,
    ProfileEvents['PartialAggregateCacheMisses'] > 0 AS has_misses
FROM system.query_log
WHERE
    type = 'QueryFinish'
    AND current_database = currentDatabase()
    AND log_comment = 'test_partial_agg_cache_multikey_q1'
    AND is_initial_query = 1
ORDER BY event_time_microseconds DESC
LIMIT 1;

SELECT date, category, avg(value), uniqExact(value)
FROM test_partial_agg_cache
GROUP BY date, category
ORDER BY date, category
SETTINGS
    use_partial_aggregate_cache = 1,
    optimize_aggregation_in_order = 0,
    max_rows_to_group_by = 0,
    group_by_overflow_mode = 'throw',
    log_comment = 'test_partial_agg_cache_multikey_q2';

SYSTEM FLUSH LOGS query_log;

SELECT
    ProfileEvents['PartialAggregateCacheHits'] > 0 AS has_hits,
    ProfileEvents['PartialAggregateCacheMisses'] = 0 AS no_misses
FROM system.query_log
WHERE
    type = 'QueryFinish'
    AND current_database = currentDatabase()
    AND log_comment = 'test_partial_agg_cache_multikey_q2'
    AND is_initial_query = 1
ORDER BY event_time_microseconds DESC
LIMIT 1;

SELECT '--- Semantic separation: same table, different WHERE';

SYSTEM DROP AGGREGATE CACHE;

SELECT category, sum(value), count()
FROM test_partial_agg_cache
WHERE date = '2024-01-01'
GROUP BY category
ORDER BY category
SETTINGS
    use_partial_aggregate_cache = 1,
    optimize_aggregation_in_order = 0,
    max_rows_to_group_by = 0,
    group_by_overflow_mode = 'throw',
    log_comment = 'test_partial_agg_cache_where_q1';

SYSTEM FLUSH LOGS query_log;

SELECT
    ProfileEvents['PartialAggregateCacheHits'] = 0 AS no_hits,
    ProfileEvents['PartialAggregateCacheMisses'] > 0 AS has_misses
FROM system.query_log
WHERE
    type = 'QueryFinish'
    AND current_database = currentDatabase()
    AND log_comment = 'test_partial_agg_cache_where_q1'
    AND is_initial_query = 1
ORDER BY event_time_microseconds DESC
LIMIT 1;

SELECT category, sum(value), count()
FROM test_partial_agg_cache
WHERE date = '2024-01-02'
GROUP BY category
ORDER BY category
SETTINGS
    use_partial_aggregate_cache = 1,
    optimize_aggregation_in_order = 0,
    max_rows_to_group_by = 0,
    group_by_overflow_mode = 'throw',
    log_comment = 'test_partial_agg_cache_where_q2';

SYSTEM FLUSH LOGS query_log;

SELECT
    ProfileEvents['PartialAggregateCacheHits'] = 0 AS no_hits,
    ProfileEvents['PartialAggregateCacheMisses'] > 0 AS has_misses
FROM system.query_log
WHERE
    type = 'QueryFinish'
    AND current_database = currentDatabase()
    AND log_comment = 'test_partial_agg_cache_where_q2'
    AND is_initial_query = 1
ORDER BY event_time_microseconds DESC
LIMIT 1;

DROP TABLE test_partial_agg_cache;
