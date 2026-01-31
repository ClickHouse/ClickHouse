-- Tags: no-fasttest
-- This test validates Statistics-based part pruning functionality with partial statistics.

DROP TABLE IF EXISTS test_stats_pruning_partial;

CREATE TABLE test_stats_pruning_partial (
    id UInt64,
    value Int64
) ENGINE = MergeTree()
PARTITION BY toInt32(id / 100)
ORDER BY id
SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi', auto_statistics_types = '';

SET use_statistics_for_part_pruning = 1;
SET allow_experimental_statistics = 1;
SET enable_analyzer = 1;
SET parallel_replicas_local_plan = 1;

-- Part 1 (OLD): id [0, 99], value [0, 99] - Partition 0
INSERT INTO test_stats_pruning_partial SELECT number, number FROM numbers(100);
-- Part 2 (OLD): id [100, 199], value [1000, 1099] - Partition 1
INSERT INTO test_stats_pruning_partial SELECT number + 100, number + 1000 FROM numbers(100);

-- ADD STATISTICS MinMax
ALTER TABLE test_stats_pruning_partial ADD STATISTICS value TYPE MinMax;

-- Part 3 (NEW with statistics): id [200, 299], value [2000, 2099] - Partition 2
INSERT INTO test_stats_pruning_partial SELECT number + 200, number + 2000 FROM numbers(100);
-- Part 4 (NEW with statistics): id [300, 399], value [3000, 3099] - Partition 3
INSERT INTO test_stats_pruning_partial SELECT number + 300, number + 3000 FROM numbers(100);

-- =============================================================================
-- Test 1: > 3000 clause
-- Part1、Part2 can not be pruned
-- =============================================================================
SELECT '-- Test 1: value > 3000';
WITH has_pr AS (SELECT count() > 0 AS is_pr FROM (EXPLAIN indexes = 1 SELECT count() FROM test_stats_pruning_partial WHERE value > 3000) WHERE explain LIKE '%ReadFromRemoteParallelReplicas%')
SELECT if((SELECT is_pr FROM has_pr), replaceRegexpOne(explain, '^    ', ''), explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_stats_pruning_partial WHERE value > 3000) WHERE explain NOT LIKE '%MergingAggregated%' AND explain NOT LIKE '%Union%' AND explain NOT LIKE '%ReadFromRemoteParallelReplicas%';
SELECT count() FROM test_stats_pruning_partial WHERE value > 3000;

-- MATERIALIZE STATISTICS MinMax
SET mutations_sync = 2;
ALTER TABLE test_stats_pruning_partial MATERIALIZE STATISTICS ALL;

-- =============================================================================
-- Test 2 after MATERIALIZE, > 3000 clause
-- Part1、Part2 should be pruned
-- =============================================================================
SELECT '-- Test 2 after MATERIALIZE: value > 3000';
WITH has_pr AS (SELECT count() > 0 AS is_pr FROM (EXPLAIN indexes = 1 SELECT count() FROM test_stats_pruning_partial WHERE value > 3000) WHERE explain LIKE '%ReadFromRemoteParallelReplicas%')
SELECT if((SELECT is_pr FROM has_pr), replaceRegexpOne(explain, '^    ', ''), explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_stats_pruning_partial WHERE value > 3000) WHERE explain NOT LIKE '%MergingAggregated%' AND explain NOT LIKE '%Union%' AND explain NOT LIKE '%ReadFromRemoteParallelReplicas%';
SELECT count() FROM test_stats_pruning_partial WHERE value > 3000;

DROP TABLE test_stats_pruning_partial;
