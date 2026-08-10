-- Tags: no-parallel, no-random-settings, no-random-merge-tree-settings, no-parallel-replicas
-- no-parallel: Messes with internal cache.
-- no-random-* / no-parallel-replicas: Flaky check must not randomize settings or inject parallel replicas; breaks GROUP BY correctness and cache ProfileEvents.

-- Partial aggregate cache: `WITH` aliases must be part of the semantic key. Without the analyzer the aggregate
-- argument names refer to the alias by name (`greater(v, threshold)`), so rebinding the same alias to another
-- expression must not reuse the per-part states of the previous binding.

SYSTEM DROP AGGREGATE CACHE;

DROP TABLE IF EXISTS test_partial_agg_cache_with_alias;

CREATE TABLE test_partial_agg_cache_with_alias
(
    k UInt32,
    v Int64
)
ENGINE = MergeTree()
ORDER BY k;

SYSTEM STOP MERGES test_partial_agg_cache_with_alias;

SET use_partial_aggregate_cache = 1;
SET optimize_aggregation_in_order = 0;
SET max_rows_to_group_by = 0;
SET group_by_overflow_mode = 'throw';

INSERT INTO test_partial_agg_cache_with_alias VALUES (1, 10), (2, 20);

SELECT '--- Constant alias, threshold 15 then 5, without the analyzer';

WITH 15 AS threshold
SELECT k, countIf(v > threshold)
FROM test_partial_agg_cache_with_alias
GROUP BY k
ORDER BY k
SETTINGS enable_analyzer = 0;

WITH 5 AS threshold
SELECT k, countIf(v > threshold)
FROM test_partial_agg_cache_with_alias
GROUP BY k
ORDER BY k
SETTINGS enable_analyzer = 0;

SELECT '--- The same with the analyzer';

WITH 15 AS threshold
SELECT k, countIf(v > threshold)
FROM test_partial_agg_cache_with_alias
GROUP BY k
ORDER BY k;

WITH 5 AS threshold
SELECT k, countIf(v > threshold)
FROM test_partial_agg_cache_with_alias
GROUP BY k
ORDER BY k;

SELECT '--- Expression alias, v + 1 then v + 2, without the analyzer';

WITH v + 1 AS x
SELECT k, sum(x)
FROM test_partial_agg_cache_with_alias
GROUP BY k
ORDER BY k
SETTINGS enable_analyzer = 0;

WITH v + 2 AS x
SELECT k, sum(x)
FROM test_partial_agg_cache_with_alias
GROUP BY k
ORDER BY k
SETTINGS enable_analyzer = 0;

SELECT '--- The same alias binding twice still hits the cache';

WITH 5 AS threshold
SELECT k, countIf(v > threshold)
FROM test_partial_agg_cache_with_alias
GROUP BY k
ORDER BY k
SETTINGS enable_analyzer = 0, log_comment = 'test_partial_agg_cache_with_alias_repeat';

SYSTEM FLUSH LOGS query_log;

SELECT
    ProfileEvents['PartialAggregateCacheHits'] > 0 AS has_hits
FROM system.query_log
WHERE
    type = 'QueryFinish'
    AND current_database = currentDatabase()
    AND log_comment = 'test_partial_agg_cache_with_alias_repeat'
    AND is_initial_query = 1
ORDER BY event_time_microseconds DESC
LIMIT 1;

DROP TABLE test_partial_agg_cache_with_alias;
