-- Tags: no-parallel, no-random-settings, no-random-merge-tree-settings, no-parallel-replicas
-- no-parallel: Messes with internal cache.
-- no-random-* / no-parallel-replicas: Flaky check must not randomize settings or inject parallel replicas; breaks GROUP BY correctness and cache ProfileEvents.

-- Partial aggregate cache: `OPTIMIZE ... FINAL` changes part identity; cache keys miss until warm again.

SYSTEM DROP AGGREGATE CACHE;

DROP TABLE IF EXISTS test_partial_agg_merge;

SELECT '--- Merge: warm cache on two parts';

CREATE TABLE test_partial_agg_merge (
    grp UInt8,
    value Int64
) ENGINE = MergeTree()
ORDER BY grp;

SYSTEM STOP MERGES test_partial_agg_merge;

SET optimize_aggregation_in_order = 0;
SET max_rows_to_group_by = 0;
SET group_by_overflow_mode = 'throw';

INSERT INTO test_partial_agg_merge SELECT 1, number FROM numbers(10000);
INSERT INTO test_partial_agg_merge SELECT 2, number FROM numbers(10000);

SELECT grp, sum(value), count()
FROM test_partial_agg_merge
GROUP BY grp
ORDER BY grp
SETTINGS
    use_partial_aggregate_cache = 1,
    optimize_aggregation_in_order = 0,
    max_rows_to_group_by = 0,
    group_by_overflow_mode = 'throw',
    log_comment = 'test_partial_agg_merge_warm1';

SYSTEM FLUSH LOGS query_log;

SELECT
    ProfileEvents['PartialAggregateCacheHits'] AS hits,
    ProfileEvents['PartialAggregateCacheMisses'] AS misses
FROM system.query_log
WHERE
    type = 'QueryFinish'
    AND current_database = currentDatabase()
    AND log_comment = 'test_partial_agg_merge_warm1'
    AND is_initial_query = 1
ORDER BY event_time_microseconds DESC
LIMIT 1;

SELECT grp, sum(value), count()
FROM test_partial_agg_merge
GROUP BY grp
ORDER BY grp
SETTINGS
    use_partial_aggregate_cache = 1,
    optimize_aggregation_in_order = 0,
    max_rows_to_group_by = 0,
    group_by_overflow_mode = 'throw',
    log_comment = 'test_partial_agg_merge_warm2';

SYSTEM FLUSH LOGS query_log;

SELECT
    ProfileEvents['PartialAggregateCacheHits'] AS hits,
    ProfileEvents['PartialAggregateCacheMisses'] AS misses
FROM system.query_log
WHERE
    type = 'QueryFinish'
    AND current_database = currentDatabase()
    AND log_comment = 'test_partial_agg_merge_warm2'
    AND is_initial_query = 1
ORDER BY event_time_microseconds DESC
LIMIT 1;

SELECT '--- Merge: single active part after OPTIMIZE FINAL';

SYSTEM START MERGES test_partial_agg_merge;
OPTIMIZE TABLE test_partial_agg_merge FINAL;
SYSTEM STOP MERGES test_partial_agg_merge;

SELECT count() AS active_parts
FROM system.parts
WHERE
    database = currentDatabase()
    AND table = 'test_partial_agg_merge'
    AND active;

SELECT '--- Merge: first query after merge (new part identity, expect no hits)';

SELECT grp, sum(value), count()
FROM test_partial_agg_merge
GROUP BY grp
ORDER BY grp
SETTINGS
    use_partial_aggregate_cache = 1,
    optimize_aggregation_in_order = 0,
    max_rows_to_group_by = 0,
    group_by_overflow_mode = 'throw',
    log_comment = 'test_partial_agg_merge_after1';

SYSTEM FLUSH LOGS query_log;

SELECT
    ProfileEvents['PartialAggregateCacheHits'] AS hits,
    ProfileEvents['PartialAggregateCacheMisses'] AS misses
FROM system.query_log
WHERE
    type = 'QueryFinish'
    AND current_database = currentDatabase()
    AND log_comment = 'test_partial_agg_merge_after1'
    AND is_initial_query = 1
ORDER BY event_time_microseconds DESC
LIMIT 1;

SELECT '--- Merge: second query after merge (warm new part)';

SELECT grp, sum(value), count()
FROM test_partial_agg_merge
GROUP BY grp
ORDER BY grp
SETTINGS
    use_partial_aggregate_cache = 1,
    optimize_aggregation_in_order = 0,
    max_rows_to_group_by = 0,
    group_by_overflow_mode = 'throw',
    log_comment = 'test_partial_agg_merge_after2';

SYSTEM FLUSH LOGS query_log;

SELECT
    ProfileEvents['PartialAggregateCacheHits'] AS hits,
    ProfileEvents['PartialAggregateCacheMisses'] AS misses
FROM system.query_log
WHERE
    type = 'QueryFinish'
    AND current_database = currentDatabase()
    AND log_comment = 'test_partial_agg_merge_after2'
    AND is_initial_query = 1
ORDER BY event_time_microseconds DESC
LIMIT 1;

DROP TABLE test_partial_agg_merge;
