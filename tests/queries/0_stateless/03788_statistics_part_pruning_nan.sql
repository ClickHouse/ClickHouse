-- This test specifically validates NaN comparison in Statistics part pruning

DROP TABLE IF EXISTS test_stats_nan;

CREATE TABLE test_stats_nan (
    dt Date,
    val_float Float64
) ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(dt)
ORDER BY tuple()
SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi', auto_statistics_types = 'minmax';

SET use_statistics_for_part_pruning = 1;
SET enable_analyzer = 1;
SET parallel_replicas_local_plan = 1;

-- =============================================================================
-- Case 1: Float64 with NaN values
-- Expected: Statistics should handle NaN correctly for = and <> comparisons
-- =============================================================================
-- Part 1: val_float [1.0, inf, 2.0, -inf, 3.0, nan, -nan]
INSERT INTO test_stats_nan SELECT
    '2025-01-01',
    arrayElement([1.0, inf, 2.0, -inf, 3.0, nan, -nan], number + 1)
FROM numbers(7);

-- For column = NaN: NaN != NaN for all values, so result should be 0
SELECT '-- Case 1: Float64 = NaN comparison (should return 0, validates Statistics NaN handling)';
WITH has_pr AS (SELECT count() > 0 AS is_pr FROM (EXPLAIN indexes = 1 SELECT count() FROM test_stats_nan WHERE val_float = nan) WHERE explain LIKE '%ReadFromRemoteParallelReplicas%')
SELECT if((SELECT is_pr FROM has_pr), replaceRegexpOne(explain, '^    ', ''), explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_stats_nan WHERE val_float = nan) WHERE explain NOT LIKE '%MergingAggregated%' AND explain NOT LIKE '%Union%' AND explain NOT LIKE '%ReadFromRemoteParallelReplicas%';
SELECT count() FROM test_stats_nan WHERE val_float = nan;

-- For column <> NaN: Since NaN != NaN for all values, ALL values including NaN should match
SELECT '-- Case 1: Float64 <> NaN comparison (should return 7, validates Statistics NaN handling)';
WITH has_pr AS (SELECT count() > 0 AS is_pr FROM (EXPLAIN indexes = 1 SELECT count() FROM test_stats_nan WHERE val_float <> nan) WHERE explain LIKE '%ReadFromRemoteParallelReplicas%')
SELECT if((SELECT is_pr FROM has_pr), replaceRegexpOne(explain, '^    ', ''), explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_stats_nan WHERE val_float <> nan) WHERE explain NOT LIKE '%MergingAggregated%' AND explain NOT LIKE '%Union%' AND explain NOT LIKE '%ReadFromRemoteParallelReplicas%';
SELECT count() FROM test_stats_nan WHERE val_float <> nan;

-- =============================================================================
-- Case 2: Float64 = -NaN comparison
-- Expected: result should be 0
-- =============================================================================
SELECT '-- Case 2: Float64 = -NaN comparison (should return 0)';
WITH has_pr AS (SELECT count() > 0 AS is_pr FROM (EXPLAIN indexes = 1 SELECT count() FROM test_stats_nan WHERE val_float = -nan) WHERE explain LIKE '%ReadFromRemoteParallelReplicas%')
SELECT if((SELECT is_pr FROM has_pr), replaceRegexpOne(explain, '^    ', ''), explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_stats_nan WHERE val_float = -nan) WHERE explain NOT LIKE '%MergingAggregated%' AND explain NOT LIKE '%Union%' AND explain NOT LIKE '%ReadFromRemoteParallelReplicas%';
SELECT count() FROM test_stats_nan WHERE val_float = -nan;

-- =============================================================================
-- Case 3: Float64 <> -NaN comparison
-- Expected: result should be 7, no value equals -NaN
-- =============================================================================
SELECT '-- Case 3: Float64 <> -NaN comparison (should return 7)';
WITH has_pr AS (SELECT count() > 0 AS is_pr FROM (EXPLAIN indexes = 1 SELECT count() FROM test_stats_nan WHERE val_float <> -nan) WHERE explain LIKE '%ReadFromRemoteParallelReplicas%')
SELECT if((SELECT is_pr FROM has_pr), replaceRegexpOne(explain, '^    ', ''), explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_stats_nan WHERE val_float <> -nan) WHERE explain NOT LIKE '%MergingAggregated%' AND explain NOT LIKE '%Union%' AND explain NOT LIKE '%ReadFromRemoteParallelReplicas%';
SELECT count() FROM test_stats_nan WHERE val_float <> -nan;

DROP TABLE test_stats_nan;
