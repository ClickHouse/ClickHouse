-- This test validates NaN comparison behavior in query execution.
-- Since 323299fe, KeyCondition folds `x = NaN` to ALWAYS_FALSE and `x <> NaN` to ALWAYS_TRUE,
-- so `= NaN` queries short-circuit before partition/statistics pruning (no Indexes in EXPLAIN),
-- while `<> NaN` queries proceed through the normal pruning pipeline.
SET explain_query_plan_default = 'legacy';

DROP TABLE IF EXISTS test_stats_nan;

CREATE TABLE test_stats_nan (
    dt Date,
    val_float Float64
) ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(dt)
ORDER BY tuple()
SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi', auto_statistics_types = 'basic';

SET use_statistics_for_part_pruning = 1;
SET parallel_replicas_local_plan = 1;
SET optimize_move_to_prewhere = 1;

-- =============================================================================
-- Case 1: Float64 with NaN values
-- For = NaN: KeyCondition folds to ALWAYS_FALSE, query short-circuits, no Indexes in EXPLAIN
-- For <> NaN: KeyCondition folds to ALWAYS_TRUE, normal pruning pipeline executes
-- =============================================================================
-- Part 1: val_float [1.0, inf, 2.0, -inf, 3.0, nan, -nan]
INSERT INTO test_stats_nan SELECT
    '2025-01-01',
    arrayElement([1.0, inf, 2.0, -inf, 3.0, nan, -nan], number + 1)
FROM numbers(7);

-- For column = NaN: KeyCondition folds to ALWAYS_FALSE, result is 0, no partition/statistics pruning
SELECT '-- Case 1: Float64 = NaN comparison (should return 0, validates Statistics NaN handling)';
SELECT explain FROM (EXPLAIN indexes = 1 SELECT count() FROM test_stats_nan WHERE val_float = nan SETTINGS enable_parallel_replicas = 0);
SELECT count() FROM test_stats_nan WHERE val_float = nan;

-- For column <> NaN: KeyCondition folds to ALWAYS_TRUE, all 7 values match
SELECT '-- Case 1: Float64 <> NaN comparison (should return 7, validates Statistics NaN handling)';
SELECT explain FROM (EXPLAIN indexes = 1 SELECT count() FROM test_stats_nan WHERE val_float <> nan SETTINGS enable_parallel_replicas = 0);
SELECT count() FROM test_stats_nan WHERE val_float <> nan;

-- =============================================================================
-- Case 2: Float64 = -NaN comparison
-- KeyCondition folds to ALWAYS_FALSE, result is 0, no partition/statistics pruning
-- =============================================================================
SELECT '-- Case 2: Float64 = -NaN comparison (should return 0)';
SELECT explain FROM (EXPLAIN indexes = 1 SELECT count() FROM test_stats_nan WHERE val_float = -nan SETTINGS enable_parallel_replicas = 0);
SELECT count() FROM test_stats_nan WHERE val_float = -nan;

-- =============================================================================
-- Case 3: Float64 <> -NaN comparison
-- KeyCondition folds to ALWAYS_TRUE, result is 7
-- =============================================================================
SELECT '-- Case 3: Float64 <> -NaN comparison (should return 7)';
SELECT explain FROM (EXPLAIN indexes = 1 SELECT count() FROM test_stats_nan WHERE val_float <> -nan SETTINGS enable_parallel_replicas = 0);
SELECT count() FROM test_stats_nan WHERE val_float <> -nan;

DROP TABLE test_stats_nan;
