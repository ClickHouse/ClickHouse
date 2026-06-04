-- Tags: no-parallel, no-random-settings, no-random-merge-tree-settings, no-parallel-replicas
-- no-parallel: Messes with internal cache.
-- no-random-* / no-parallel-replicas: Flaky check must not randomize settings or inject parallel replicas.

-- Regression: one physical part is read in many chunks. A PAC hit merges the full per-part
-- aggregate once; later chunks with the same part key are skipped and must not change the result.

SYSTEM DROP PARTIAL AGGREGATE CACHE;

DROP TABLE IF EXISTS test_partial_agg_multi_chunk;

CREATE TABLE test_partial_agg_multi_chunk (
    grp UInt8,
    value Int64
) ENGINE = MergeTree()
ORDER BY grp;

SYSTEM STOP MERGES test_partial_agg_multi_chunk;

SET optimize_aggregation_in_order = 0;
SET max_rows_to_group_by = 0;
SET group_by_overflow_mode = 'throw';
SET max_block_size = 1000;
SET preferred_block_size_bytes = 8192;
SET max_threads = 4;

INSERT INTO test_partial_agg_multi_chunk SELECT 0, number FROM numbers(100000);

SELECT '--- Baseline without cache';

SELECT grp, sum(value), count()
FROM test_partial_agg_multi_chunk
GROUP BY grp
ORDER BY grp
SETTINGS
    use_partial_aggregate_cache = 0,
    optimize_aggregation_in_order = 0,
    max_rows_to_group_by = 0,
    group_by_overflow_mode = 'throw',
    max_block_size = 1000,
    preferred_block_size_bytes = 8192,
    max_threads = 4,
    log_comment = 'test_partial_agg_multi_chunk_baseline';

SELECT '--- Warm cache (miss, multi-chunk read)';

SELECT grp, sum(value), count()
FROM test_partial_agg_multi_chunk
GROUP BY grp
ORDER BY grp
SETTINGS
    use_partial_aggregate_cache = 1,
    optimize_aggregation_in_order = 0,
    max_rows_to_group_by = 0,
    group_by_overflow_mode = 'throw',
    max_block_size = 1000,
    preferred_block_size_bytes = 8192,
    max_threads = 4,
    log_comment = 'test_partial_agg_multi_chunk_warm';

SYSTEM FLUSH LOGS query_log;

SELECT
    ProfileEvents['PartialAggregateCacheHits'] AS hits,
    ProfileEvents['PartialAggregateCacheMisses'] AS misses
FROM system.query_log
WHERE
    type = 'QueryFinish'
    AND current_database = currentDatabase()
    AND log_comment = 'test_partial_agg_multi_chunk_warm'
    AND is_initial_query = 1
ORDER BY event_time_microseconds DESC
LIMIT 1;

SELECT '--- Cache hit (skip later chunks for same part key)';

SELECT grp, sum(value), count()
FROM test_partial_agg_multi_chunk
GROUP BY grp
ORDER BY grp
SETTINGS
    use_partial_aggregate_cache = 1,
    optimize_aggregation_in_order = 0,
    max_rows_to_group_by = 0,
    group_by_overflow_mode = 'throw',
    max_block_size = 1000,
    preferred_block_size_bytes = 8192,
    max_threads = 4,
    log_comment = 'test_partial_agg_multi_chunk_hit';

SYSTEM FLUSH LOGS query_log;

SELECT
    ProfileEvents['PartialAggregateCacheHits'] AS hits,
    ProfileEvents['PartialAggregateCacheMisses'] AS misses
FROM system.query_log
WHERE
    type = 'QueryFinish'
    AND current_database = currentDatabase()
    AND log_comment = 'test_partial_agg_multi_chunk_hit'
    AND is_initial_query = 1
ORDER BY event_time_microseconds DESC
LIMIT 1;

DROP TABLE test_partial_agg_multi_chunk;
