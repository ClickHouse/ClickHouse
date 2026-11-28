-- Tags: no-parallel, no-random-settings, no-random-merge-tree-settings, no-parallel-replicas
-- no-parallel: Messes with internal cache.
-- no-random-* / no-parallel-replicas: Flaky check must not randomize settings or inject parallel replicas; breaks GROUP BY correctness and cache ProfileEvents.

-- Partial aggregate cache: a repeated aggregation over `ARRAY JOIN` must not lose the cached parts.
-- A plan-time hit is represented by a zero-row chunk carrying `PartialAggregatePlanHitInfo`, which
-- `ArrayJoinTransform` would rebuild without preserving `ChunkInfos` (and drop entirely, being zero-row),
-- so plan-time probing is disabled for such plans and every run must return the same result.

SYSTEM DROP AGGREGATE CACHE;

DROP TABLE IF EXISTS test_partial_agg_cache_array_join_repeat;

CREATE TABLE test_partial_agg_cache_array_join_repeat
(
    id UInt32,
    arr Array(UInt32),
    value Int64
)
ENGINE = MergeTree()
ORDER BY id;

SYSTEM STOP MERGES test_partial_agg_cache_array_join_repeat;

SET optimize_aggregation_in_order = 0;
SET max_rows_to_group_by = 0;
SET group_by_overflow_mode = 'throw';

-- Two parts, so a lost hit of one part would be visible in the result.
INSERT INTO test_partial_agg_cache_array_join_repeat VALUES (1, [10, 11], 10), (2, [20], 20);
INSERT INTO test_partial_agg_cache_array_join_repeat VALUES (3, [30, 31, 32], 30), (4, [40], 40);

SELECT '--- Reference result without the cache';

SELECT id, sum(value), count()
FROM test_partial_agg_cache_array_join_repeat
ARRAY JOIN arr
GROUP BY id
ORDER BY id
SETTINGS use_partial_aggregate_cache = 0;

SELECT '--- First run with the cache';

SELECT id, sum(value), count()
FROM test_partial_agg_cache_array_join_repeat
ARRAY JOIN arr
GROUP BY id
ORDER BY id
SETTINGS use_partial_aggregate_cache = 1;

SELECT '--- Second run with the cache (must be identical)';

SELECT id, sum(value), count()
FROM test_partial_agg_cache_array_join_repeat
ARRAY JOIN arr
GROUP BY id
ORDER BY id
SETTINGS use_partial_aggregate_cache = 1;

SELECT '--- Third run with the cache (must be identical)';

SELECT id, sum(value), count()
FROM test_partial_agg_cache_array_join_repeat
ARRAY JOIN arr
GROUP BY id
ORDER BY id
SETTINGS use_partial_aggregate_cache = 1;

SELECT '--- The same for the arrayJoin function';

SELECT id, sum(value), count()
FROM test_partial_agg_cache_array_join_repeat
GROUP BY id, arrayJoin(arr)
ORDER BY id
SETTINGS use_partial_aggregate_cache = 0;

SELECT id, sum(value), count()
FROM test_partial_agg_cache_array_join_repeat
GROUP BY id, arrayJoin(arr)
ORDER BY id
SETTINGS use_partial_aggregate_cache = 1;

SELECT id, sum(value), count()
FROM test_partial_agg_cache_array_join_repeat
GROUP BY id, arrayJoin(arr)
ORDER BY id
SETTINGS use_partial_aggregate_cache = 1;

DROP TABLE test_partial_agg_cache_array_join_repeat;
