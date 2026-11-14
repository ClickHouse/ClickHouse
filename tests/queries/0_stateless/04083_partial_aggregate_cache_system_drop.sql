-- Tags: no-parallel, no-random-settings, no-random-merge-tree-settings, no-parallel-replicas
-- no-parallel: Messes with internal cache.
-- no-random-* / no-parallel-replicas: Flaky check must not randomize settings or inject parallel replicas; breaks GROUP BY correctness and cache ProfileEvents.

-- Partial aggregate cache: `SYSTEM DROP AGGREGATE CACHE` clears entries; next query misses then warms again.

SYSTEM DROP AGGREGATE CACHE;

DROP TABLE IF EXISTS test_partial_agg_systrunc;

SELECT '--- SYSTEM DROP: warm cache then clear';

CREATE TABLE test_partial_agg_systrunc (
    grp UInt8,
    value Int64
) ENGINE = MergeTree()
ORDER BY grp;

SYSTEM STOP MERGES test_partial_agg_systrunc;

SET optimize_aggregation_in_order = 0;
SET max_rows_to_group_by = 0;
SET group_by_overflow_mode = 'throw';

INSERT INTO test_partial_agg_systrunc SELECT 1, number FROM numbers(10000);
INSERT INTO test_partial_agg_systrunc SELECT 2, number FROM numbers(10000);

SELECT grp, sum(value), count()
FROM test_partial_agg_systrunc
GROUP BY grp
ORDER BY grp
SETTINGS
    use_partial_aggregate_cache = 1,
    optimize_aggregation_in_order = 0,
    max_rows_to_group_by = 0,
    group_by_overflow_mode = 'throw',
    log_comment = 'test_partial_agg_systrunc_warm1';

SYSTEM FLUSH LOGS query_log;

SELECT
    ProfileEvents['PartialAggregateCacheHits'] AS hits,
    ProfileEvents['PartialAggregateCacheMisses'] AS misses
FROM system.query_log
WHERE
    type = 'QueryFinish'
    AND current_database = currentDatabase()
    AND log_comment = 'test_partial_agg_systrunc_warm1'
    AND is_initial_query = 1
ORDER BY event_time_microseconds DESC
LIMIT 1;

SELECT grp, sum(value), count()
FROM test_partial_agg_systrunc
GROUP BY grp
ORDER BY grp
SETTINGS
    use_partial_aggregate_cache = 1,
    optimize_aggregation_in_order = 0,
    max_rows_to_group_by = 0,
    group_by_overflow_mode = 'throw',
    log_comment = 'test_partial_agg_systrunc_warm2';

SYSTEM FLUSH LOGS query_log;

SELECT
    ProfileEvents['PartialAggregateCacheHits'] AS hits,
    ProfileEvents['PartialAggregateCacheMisses'] AS misses
FROM system.query_log
WHERE
    type = 'QueryFinish'
    AND current_database = currentDatabase()
    AND log_comment = 'test_partial_agg_systrunc_warm2'
    AND is_initial_query = 1
ORDER BY event_time_microseconds DESC
LIMIT 1;

SYSTEM DROP AGGREGATE CACHE;

SELECT '--- SYSTEM DROP: first query after drop (all miss)';

SELECT grp, sum(value), count()
FROM test_partial_agg_systrunc
GROUP BY grp
ORDER BY grp
SETTINGS
    use_partial_aggregate_cache = 1,
    optimize_aggregation_in_order = 0,
    max_rows_to_group_by = 0,
    group_by_overflow_mode = 'throw',
    log_comment = 'test_partial_agg_systrunc_drop1';

SYSTEM FLUSH LOGS query_log;

SELECT
    ProfileEvents['PartialAggregateCacheHits'] AS hits,
    ProfileEvents['PartialAggregateCacheMisses'] AS misses
FROM system.query_log
WHERE
    type = 'QueryFinish'
    AND current_database = currentDatabase()
    AND log_comment = 'test_partial_agg_systrunc_drop1'
    AND is_initial_query = 1
ORDER BY event_time_microseconds DESC
LIMIT 1;

SELECT '--- SYSTEM DROP: second query (hits again)';

SELECT grp, sum(value), count()
FROM test_partial_agg_systrunc
GROUP BY grp
ORDER BY grp
SETTINGS
    use_partial_aggregate_cache = 1,
    optimize_aggregation_in_order = 0,
    max_rows_to_group_by = 0,
    group_by_overflow_mode = 'throw',
    log_comment = 'test_partial_agg_systrunc_drop2';

SYSTEM FLUSH LOGS query_log;

SELECT
    ProfileEvents['PartialAggregateCacheHits'] AS hits,
    ProfileEvents['PartialAggregateCacheMisses'] AS misses
FROM system.query_log
WHERE
    type = 'QueryFinish'
    AND current_database = currentDatabase()
    AND log_comment = 'test_partial_agg_systrunc_drop2'
    AND is_initial_query = 1
ORDER BY event_time_microseconds DESC
LIMIT 1;

DROP TABLE test_partial_agg_systrunc;
