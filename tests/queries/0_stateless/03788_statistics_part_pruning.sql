-- This test validates Statistics-based part pruning functionality.

DROP TABLE IF EXISTS test_stats_pruning;

CREATE TABLE test_stats_pruning (
    dt Date,
    id UInt64,
    value Int64,
    value_nullable Nullable(Int64),
    value_decimal Decimal128(0),
    str String,
    version UInt64
) ENGINE = ReplacingMergeTree(version)
PARTITION BY toYYYYMMDD(dt)
ORDER BY id
SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi', auto_statistics_types = 'minmax';

SET use_statistics_for_part_pruning = 1;
SET enable_analyzer = 1;
SET parallel_replicas_local_plan = 1;

-- Part 1: 202501, id [0, 99], value [0, 99], version [0, 99]
INSERT INTO test_stats_pruning SELECT '2025-01-11', number, number, number, toDecimal128(number, 0), 'a', number FROM numbers(100);
-- Part 2: 202501, id [100, 199], value [1000, 1099], version [1000, 1099]
INSERT INTO test_stats_pruning SELECT '2025-01-12', number + 100, number + 1000, number + 1000, toDecimal128(number + 1000, 0), 'b', number + 1000 FROM numbers(100);
-- Part 3: 202501, id [200, 299], value [2000, 2099], value_nullable NULL, version [2000, 2099]
INSERT INTO test_stats_pruning SELECT '2025-01-13', number + 200, number + 2000, NULL, toDecimal128(number + 2000, 0), 'c', number + 2000 FROM numbers(100);
-- Part 4: 202501, id [300, 399], value [3000, 3099], version [3000, 3099]
INSERT INTO test_stats_pruning SELECT '2025-01-14', number + 300, number + 3000, number + 3000, toDecimal128(number + 3000, 0), 'd', number + 3000 FROM numbers(100);

-- =============================================================================
-- Test 1: AND on different columns all have statistics, should prune parts
-- =============================================================================
-- Query: id >= 50 AND NOT(value > 500) AND version IN (50, 1050)
-- Expected: Only Part 1 matches (id range [0,99], value [0,99], version [0,99])
-- Statistics pruning should reduce parts from 4 to 1
SELECT '-- AND on different columns all have statistics, should prune parts';
WITH has_pr AS (SELECT count() > 0 AS is_pr FROM (EXPLAIN indexes = 1 SELECT count() FROM test_stats_pruning WHERE id >= 50 AND NOT(value > 500) AND version IN (50, 1050)) WHERE explain LIKE '%ReadFromRemoteParallelReplicas%')
SELECT if((SELECT is_pr FROM has_pr), replaceRegexpOne(explain, '^    ', ''), explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_stats_pruning WHERE id >= 50 AND NOT(value > 500) AND version IN (50, 1050)) WHERE explain NOT LIKE '%MergingAggregated%' AND explain NOT LIKE '%Union%' AND explain NOT LIKE '%ReadFromRemoteParallelReplicas%';
SELECT count() FROM test_stats_pruning WHERE id >= 50 AND NOT(value > 500) AND version IN (50, 1050);

-- =============================================================================
-- Test 2: OR on different columns all have statistics, should prune parts
-- =============================================================================
-- Query: id < 50 OR version > 3050
-- Expected: Parts 1 and 4 match (Part 1: id<50, Part 4: version>3050)
-- Statistics pruning should reduce parts from 4 to 2
SELECT '-- OR on different columns all have statistics, should prune parts';
WITH has_pr AS (SELECT count() > 0 AS is_pr FROM (EXPLAIN indexes = 1 SELECT count() FROM test_stats_pruning WHERE id < 50 OR version > 3050) WHERE explain LIKE '%ReadFromRemoteParallelReplicas%')
SELECT if((SELECT is_pr FROM has_pr), replaceRegexpOne(explain, '^    ', ''), explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_stats_pruning WHERE id < 50 OR version > 3050) WHERE explain NOT LIKE '%MergingAggregated%' AND explain NOT LIKE '%Union%' AND explain NOT LIKE '%ReadFromRemoteParallelReplicas%';
SELECT count() FROM test_stats_pruning WHERE id < 50 OR version > 3050;

-- =============================================================================
-- Test 3: AND with column without statistics (str has no minmax), should prune parts
-- =============================================================================
-- Query: value = 50 AND str = 'a'
-- Expected: Only Part 1 matches (value=50 exists in Part 1)
-- Statistics on 'value' can still prune parts even though 'str' has no statistics
SELECT '-- AND with column without statistics (str has no minmax), should prune parts';
WITH has_pr AS (SELECT count() > 0 AS is_pr FROM (EXPLAIN indexes = 1 SELECT count() FROM test_stats_pruning WHERE value = 50 AND str = 'a') WHERE explain LIKE '%ReadFromRemoteParallelReplicas%')
SELECT if((SELECT is_pr FROM has_pr), replaceRegexpOne(explain, '^    ', ''), explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_stats_pruning WHERE value = 50 AND str = 'a') WHERE explain NOT LIKE '%MergingAggregated%' AND explain NOT LIKE '%Union%' AND explain NOT LIKE '%ReadFromRemoteParallelReplicas%';
SELECT count() FROM test_stats_pruning WHERE value = 50 AND str = 'a';

-- =============================================================================
-- Test 4: OR with column without statistics (str has no minmax), should NOT prune parts
-- =============================================================================
-- Query: value = 50 OR str = 'x'
-- Expected: Cannot prune any parts because OR requires all branches to be safe
-- Statistics pruning should not reduce parts (4/4 remain)
SELECT '-- OR with column without statistics (str has no minmax), should NOT prune parts';
WITH has_pr AS (SELECT count() > 0 AS is_pr FROM (EXPLAIN indexes = 1 SELECT count() FROM test_stats_pruning WHERE value = 50 OR str = 'x') WHERE explain LIKE '%ReadFromRemoteParallelReplicas%')
SELECT if((SELECT is_pr FROM has_pr), replaceRegexpOne(explain, '^    ', ''), explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_stats_pruning WHERE value = 50 OR str = 'x') WHERE explain NOT LIKE '%MergingAggregated%' AND explain NOT LIKE '%Union%' AND explain NOT LIKE '%ReadFromRemoteParallelReplicas%';
SELECT count() FROM test_stats_pruning WHERE value = 50 OR str = 'x';

-- =============================================================================
-- Test 5: Partition and statistics pruning combined
-- =============================================================================
-- Query: dt = '2025-01-11' AND value = 1000
-- Expected: No rows match (Part 1 has dt='2025-01-11' but value=[0,99], value=1000 not in range)
-- Statistics should prune based on partition first, then statistics
SELECT '-- partition and statistics pruning';
WITH has_pr AS (SELECT count() > 0 AS is_pr FROM (EXPLAIN indexes = 1 SELECT count() FROM test_stats_pruning WHERE dt = '2025-01-11' AND value = 1000) WHERE explain LIKE '%ReadFromRemoteParallelReplicas%')
SELECT if((SELECT is_pr FROM has_pr), replaceRegexpOne(explain, '^    ', ''), explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_stats_pruning WHERE dt = '2025-01-11' AND value = 1000) WHERE explain NOT LIKE '%MergingAggregated%' AND explain NOT LIKE '%Union%' AND explain NOT LIKE '%ReadFromRemoteParallelReplicas%';
SELECT count() FROM test_stats_pruning WHERE dt = '2025-01-11' AND value = 1000;

-- =============================================================================
-- Test 6: Nullable column pruning - Part 3 (all NULL) should be pruned
-- =============================================================================
-- Query: value_nullable >= 3000 AND value_nullable <= 3050
-- Due to missing info on actual NULL values within the column, only entirely-null parts can be pruned
SELECT '-- Nullable column pruning, Part 3 has NULL values, should be pruned';
WITH has_pr AS (SELECT count() > 0 AS is_pr FROM (EXPLAIN indexes = 1 SELECT count() FROM test_stats_pruning WHERE value_nullable >= 3000 AND value_nullable <= 3050) WHERE explain LIKE '%ReadFromRemoteParallelReplicas%')
SELECT if((SELECT is_pr FROM has_pr), replaceRegexpOne(explain, '^    ', ''), explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_stats_pruning WHERE value_nullable >= 3000 AND value_nullable <= 3050) WHERE explain NOT LIKE '%MergingAggregated%' AND explain NOT LIKE '%Union%' AND explain NOT LIKE '%ReadFromRemoteParallelReplicas%';
SELECT count() FROM test_stats_pruning WHERE value_nullable >= 3000 AND value_nullable <= 3050;

-- =============================================================================
-- Test 7: FINAL query should NOT use Statistics pruning
-- =============================================================================
-- Query: SELECT count() FROM test_stats_pruning FINAL WHERE value = 1050
-- Expected: Statistics pruning is disabled for FINAL queries
-- All 4 parts should be read (no pruning)
SELECT '-- FINAL query should NOT use Statistics pruning';
WITH has_pr AS (SELECT count() > 0 AS is_pr FROM (EXPLAIN indexes = 1 SELECT count() FROM test_stats_pruning FINAL WHERE value = 1050) WHERE explain LIKE '%ReadFromRemoteParallelReplicas%')
SELECT if((SELECT is_pr FROM has_pr), replaceRegexpOne(explain, '^    ', ''), explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_stats_pruning FINAL WHERE value = 1050) WHERE explain NOT LIKE '%MergingAggregated%' AND explain NOT LIKE '%Union%' AND explain NOT LIKE '%ReadFromRemoteParallelReplicas%';
SELECT count() FROM test_stats_pruning FINAL WHERE value = 1050;

-- =============================================================================
-- Test 8: Non-monotonic function wrapping column, should NOT use Statistics pruning
-- =============================================================================
-- Query: length(toString(value)) = 2
-- Expected: Statistics pruning is disabled for non-column expressions
-- All 4 parts should be read (no pruning)
SELECT '-- Non-monotonic function wrapping column length(toString(value)), should NOT use Statistics pruning';
WITH has_pr AS (SELECT count() > 0 AS is_pr FROM (EXPLAIN indexes = 1 SELECT count() FROM test_stats_pruning WHERE length(toString(value)) = 2) WHERE explain LIKE '%ReadFromRemoteParallelReplicas%')
SELECT if((SELECT is_pr FROM has_pr), replaceRegexpOne(explain, '^    ', ''), explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_stats_pruning WHERE length(toString(value)) = 2) WHERE explain NOT LIKE '%MergingAggregated%' AND explain NOT LIKE '%Union%' AND explain NOT LIKE '%ReadFromRemoteParallelReplicas%';
SELECT count() FROM test_stats_pruning WHERE length(toString(value)) = 2;

DROP TABLE test_stats_pruning;
