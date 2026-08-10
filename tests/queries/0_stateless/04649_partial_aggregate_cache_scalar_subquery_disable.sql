-- Tags: no-parallel, no-random-settings, no-random-merge-tree-settings, no-parallel-replicas
-- no-parallel: Messes with internal cache.
-- no-random-* / no-parallel-replicas: Flaky check must not randomize settings or inject parallel replicas; breaks GROUP BY correctness and cache ProfileEvents.

-- Partial aggregate cache: a scalar subquery feeding the pre-aggregation expressions must not make the cached
-- per-part states stale when only the subquery source changes. The subquery is folded into a constant before
-- the cache key is computed, so its value participates in the key through the aggregate arguments and the
-- grouping keys; any subquery left in the query AST additionally disables the key fail-close.

SYSTEM DROP AGGREGATE CACHE;

DROP TABLE IF EXISTS test_partial_agg_cache_scalar_fact;
DROP TABLE IF EXISTS test_partial_agg_cache_scalar_dim;

CREATE TABLE test_partial_agg_cache_scalar_fact
(
    k UInt32,
    v Int64
)
ENGINE = MergeTree()
ORDER BY k;

CREATE TABLE test_partial_agg_cache_scalar_dim
(
    m Int64
)
ENGINE = MergeTree()
ORDER BY m;

SYSTEM STOP MERGES test_partial_agg_cache_scalar_fact;

SET optimize_aggregation_in_order = 0;
SET max_rows_to_group_by = 0;
SET group_by_overflow_mode = 'throw';
SET use_partial_aggregate_cache = 1;

INSERT INTO test_partial_agg_cache_scalar_fact VALUES (1, 10), (2, 20);
INSERT INTO test_partial_agg_cache_scalar_dim VALUES (2);

SELECT '--- Aggregate argument, divisor 2';

SELECT k, sum(intDiv(v, (SELECT m FROM test_partial_agg_cache_scalar_dim LIMIT 1)))
FROM test_partial_agg_cache_scalar_fact
GROUP BY k
ORDER BY k;

SELECT '--- Only the dimension table changes, divisor 10';

TRUNCATE TABLE test_partial_agg_cache_scalar_dim;
INSERT INTO test_partial_agg_cache_scalar_dim VALUES (10);

SELECT k, sum(intDiv(v, (SELECT m FROM test_partial_agg_cache_scalar_dim LIMIT 1)))
FROM test_partial_agg_cache_scalar_fact
GROUP BY k
ORDER BY k;

SELECT k, sum(intDiv(v, (SELECT m FROM test_partial_agg_cache_scalar_dim LIMIT 1)))
FROM test_partial_agg_cache_scalar_fact
GROUP BY k
ORDER BY k
SETTINGS use_partial_aggregate_cache = 0;

SELECT '--- Filter inside the aggregate function, threshold 15 then 5';

TRUNCATE TABLE test_partial_agg_cache_scalar_dim;
INSERT INTO test_partial_agg_cache_scalar_dim VALUES (15);

SELECT k, countIf(v > (SELECT m FROM test_partial_agg_cache_scalar_dim LIMIT 1))
FROM test_partial_agg_cache_scalar_fact
GROUP BY k
ORDER BY k;

TRUNCATE TABLE test_partial_agg_cache_scalar_dim;
INSERT INTO test_partial_agg_cache_scalar_dim VALUES (5);

SELECT k, countIf(v > (SELECT m FROM test_partial_agg_cache_scalar_dim LIMIT 1))
FROM test_partial_agg_cache_scalar_fact
GROUP BY k
ORDER BY k;

SELECT '--- Grouping key, 15 then 5';

TRUNCATE TABLE test_partial_agg_cache_scalar_dim;
INSERT INTO test_partial_agg_cache_scalar_dim VALUES (15);

SELECT k, (SELECT m FROM test_partial_agg_cache_scalar_dim LIMIT 1) AS g, sum(v)
FROM test_partial_agg_cache_scalar_fact
GROUP BY k, g
ORDER BY k;

TRUNCATE TABLE test_partial_agg_cache_scalar_dim;
INSERT INTO test_partial_agg_cache_scalar_dim VALUES (5);

SELECT k, (SELECT m FROM test_partial_agg_cache_scalar_dim LIMIT 1) AS g, sum(v)
FROM test_partial_agg_cache_scalar_fact
GROUP BY k, g
ORDER BY k;

SELECT '--- The same without the analyzer';

TRUNCATE TABLE test_partial_agg_cache_scalar_dim;
INSERT INTO test_partial_agg_cache_scalar_dim VALUES (15);

SELECT k, countIf(v > (SELECT m FROM test_partial_agg_cache_scalar_dim LIMIT 1))
FROM test_partial_agg_cache_scalar_fact
GROUP BY k
ORDER BY k
SETTINGS enable_analyzer = 0;

TRUNCATE TABLE test_partial_agg_cache_scalar_dim;
INSERT INTO test_partial_agg_cache_scalar_dim VALUES (5);

SELECT k, countIf(v > (SELECT m FROM test_partial_agg_cache_scalar_dim LIMIT 1))
FROM test_partial_agg_cache_scalar_fact
GROUP BY k
ORDER BY k
SETTINGS enable_analyzer = 0;

DROP TABLE test_partial_agg_cache_scalar_fact;
DROP TABLE test_partial_agg_cache_scalar_dim;
