-- Tags: no-parallel, no-random-settings, no-random-merge-tree-settings, no-parallel-replicas
-- no-parallel: Messes with internal cache.
-- no-random-* / no-parallel-replicas: Flaky check must not randomize settings or inject parallel replicas; breaks GROUP BY correctness and cache ProfileEvents.

-- Partial aggregate cache: `FINAL` must not use the cache at all, at planning or at execution time.
-- Which rows of a part survive the merge depends on the other parts, and vertical `FINAL` can emit a subset
-- of a source chunk while keeping its `PartialAggregateInfo`, so per-part states keyed by part identity alone
-- become stale as soon as an overlapping part appears.

SYSTEM DROP AGGREGATE CACHE;

DROP TABLE IF EXISTS test_partial_agg_cache_final;

CREATE TABLE test_partial_agg_cache_final
(
    id UInt32,
    ver UInt32,
    val Int64
)
ENGINE = ReplacingMergeTree(ver)
ORDER BY id;

SYSTEM STOP MERGES test_partial_agg_cache_final;

SET use_partial_aggregate_cache = 1;
SET optimize_aggregation_in_order = 0;
SET max_rows_to_group_by = 0;
SET group_by_overflow_mode = 'throw';

INSERT INTO test_partial_agg_cache_final VALUES (1, 1, 100), (2, 1, 200);

SELECT '--- FINAL warms nothing';

SELECT sum(val) FROM test_partial_agg_cache_final FINAL SETTINGS log_comment = 'test_partial_agg_cache_final_warm';

SYSTEM FLUSH LOGS query_log;

SELECT
    ProfileEvents['PartialAggregateCacheHits'] AS hits,
    ProfileEvents['PartialAggregateCacheMisses'] AS misses
FROM system.query_log
WHERE
    type = 'QueryFinish'
    AND current_database = currentDatabase()
    AND log_comment = 'test_partial_agg_cache_final_warm'
    AND is_initial_query = 1
ORDER BY event_time_microseconds DESC
LIMIT 1;

SELECT '--- An overlapping part changes which rows survive: 201, not 301';

INSERT INTO test_partial_agg_cache_final VALUES (1, 2, 1);

SELECT sum(val) FROM test_partial_agg_cache_final FINAL SETTINGS log_comment = 'test_partial_agg_cache_final_second';
SELECT sum(val) FROM test_partial_agg_cache_final FINAL SETTINGS enable_vertical_final = 1;
SELECT sum(val) FROM test_partial_agg_cache_final FINAL SETTINGS use_partial_aggregate_cache = 0;

SYSTEM FLUSH LOGS query_log;

SELECT
    ProfileEvents['PartialAggregateCacheHits'] AS hits,
    ProfileEvents['PartialAggregateCacheMisses'] AS misses
FROM system.query_log
WHERE
    type = 'QueryFinish'
    AND current_database = currentDatabase()
    AND log_comment = 'test_partial_agg_cache_final_second'
    AND is_initial_query = 1
ORDER BY event_time_microseconds DESC
LIMIT 1;

SELECT '--- Without FINAL the same table still uses the cache';

SELECT sum(val) FROM test_partial_agg_cache_final SETTINGS log_comment = 'test_partial_agg_cache_final_no_final_warm';
SELECT sum(val) FROM test_partial_agg_cache_final SETTINGS log_comment = 'test_partial_agg_cache_final_no_final_hit';

SYSTEM FLUSH LOGS query_log;

SELECT
    ProfileEvents['PartialAggregateCacheHits'] > 0 AS has_hits
FROM system.query_log
WHERE
    type = 'QueryFinish'
    AND current_database = currentDatabase()
    AND log_comment = 'test_partial_agg_cache_final_no_final_hit'
    AND is_initial_query = 1
ORDER BY event_time_microseconds DESC
LIMIT 1;

DROP TABLE test_partial_agg_cache_final;
