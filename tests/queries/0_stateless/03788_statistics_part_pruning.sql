DROP TABLE IF EXISTS test_stats_pruning;

CREATE TABLE test_stats_pruning (
    dt Date,
    id UInt64,
    value Int64,
    value_nullable Nullable(Int64),
    str String,
    version UInt64
) ENGINE = ReplacingMergeTree(version)
PARTITION BY toYYYYMM(dt)
ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, auto_statistics_types = 'minmax';

SET allow_statistics_optimize = 1;

-- Part 1: 202501, id [0, 99], value [0, 99], version [0, 99]
INSERT INTO test_stats_pruning SELECT '2025-01-11', number, number, number, 'a', number FROM numbers(100);
-- Part 2: 202501, id [100, 199], value [1000, 1099], version [1000, 1099]
INSERT INTO test_stats_pruning SELECT '2025-01-12', number + 100, number + 1000, number + 1000, 'b', number + 1000 FROM numbers(100);
-- Part 3: 202501, id [200, 299], value [2000, 2099], value_nullable NULL, version [2000, 2099]
INSERT INTO test_stats_pruning SELECT '2025-01-13', number + 200, number + 2000, NULL, 'c', number + 2000 FROM numbers(100);
-- Part 4: 202501, id [300, 399], value [3000, 3099], version [3000, 3099]
INSERT INTO test_stats_pruning SELECT '2025-01-14', number + 300, number + 3000, number + 3000, 'd', number + 3000 FROM numbers(100);

-- AND on different columns all have statistics, should prune parts
WITH has_pr AS (SELECT count() > 0 AS is_pr FROM (EXPLAIN indexes = 1 SELECT count() FROM test_stats_pruning WHERE id >= 50 AND NOT(value > 500) AND version IN (50, 1050)) WHERE explain LIKE '%ReadFromRemoteParallelReplicas%')
SELECT if((SELECT is_pr FROM has_pr), replaceRegexpOne(explain, '^    ', ''), explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_stats_pruning WHERE id >= 50 AND NOT(value > 500) AND version IN (50, 1050)) WHERE explain NOT LIKE '%MergingAggregated%' AND explain NOT LIKE '%Union%' AND explain NOT LIKE '%ReadFromRemoteParallelReplicas%';
SELECT count() FROM test_stats_pruning WHERE id >= 50 AND NOT(value > 500) AND version IN (50, 1050);

-- OR on different columns all have statistics, should prune parts
WITH has_pr AS (SELECT count() > 0 AS is_pr FROM (EXPLAIN indexes = 1 SELECT count() FROM test_stats_pruning WHERE id < 50 OR version > 3050) WHERE explain LIKE '%ReadFromRemoteParallelReplicas%')
SELECT if((SELECT is_pr FROM has_pr), replaceRegexpOne(explain, '^    ', ''), explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_stats_pruning WHERE id < 50 OR version > 3050) WHERE explain NOT LIKE '%MergingAggregated%' AND explain NOT LIKE '%Union%' AND explain NOT LIKE '%ReadFromRemoteParallelReplicas%';
SELECT count() FROM test_stats_pruning WHERE id < 50 OR version > 3050;

-- AND with column without statistics (str has no minmax), should prune parts
WITH has_pr AS (SELECT count() > 0 AS is_pr FROM (EXPLAIN indexes = 1 SELECT count() FROM test_stats_pruning WHERE value = 50 AND str = 'a') WHERE explain LIKE '%ReadFromRemoteParallelReplicas%')
SELECT if((SELECT is_pr FROM has_pr), replaceRegexpOne(explain, '^    ', ''), explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_stats_pruning WHERE value = 50 AND str = 'a') WHERE explain NOT LIKE '%MergingAggregated%' AND explain NOT LIKE '%Union%' AND explain NOT LIKE '%ReadFromRemoteParallelReplicas%';
SELECT count() FROM test_stats_pruning WHERE value = 50 AND str = 'a';

-- OR with column without statistics (str has no minmax), should NOT prune parts
WITH has_pr AS (SELECT count() > 0 AS is_pr FROM (EXPLAIN indexes = 1 SELECT count() FROM test_stats_pruning WHERE value = 50 OR str = 'x') WHERE explain LIKE '%ReadFromRemoteParallelReplicas%')
SELECT if((SELECT is_pr FROM has_pr), replaceRegexpOne(explain, '^    ', ''), explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_stats_pruning WHERE value = 50 OR str = 'x') WHERE explain NOT LIKE '%MergingAggregated%' AND explain NOT LIKE '%Union%' AND explain NOT LIKE '%ReadFromRemoteParallelReplicas%';
SELECT count() FROM test_stats_pruning WHERE value = 50 OR str = 'x';

-- partition and statistics pruning
WITH has_pr AS (SELECT count() > 0 AS is_pr FROM (EXPLAIN indexes = 1 SELECT count() FROM test_stats_pruning WHERE dt = '2025-01-11' AND value = 1000) WHERE explain LIKE '%ReadFromRemoteParallelReplicas%')
SELECT if((SELECT is_pr FROM has_pr), replaceRegexpOne(explain, '^    ', ''), explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_stats_pruning WHERE dt = '2025-01-11' AND value = 1000) WHERE explain NOT LIKE '%MergingAggregated%' AND explain NOT LIKE '%Union%' AND explain NOT LIKE '%ReadFromRemoteParallelReplicas%';
SELECT count() FROM test_stats_pruning WHERE dt = '2025-01-11' AND value = 1000;

-- Nullable column pruning, Part 3 has NULL values, should be pruned
WITH has_pr AS (SELECT count() > 0 AS is_pr FROM (EXPLAIN indexes = 1 SELECT count() FROM test_stats_pruning WHERE value_nullable >= 3000 AND value_nullable <= 3050) WHERE explain LIKE '%ReadFromRemoteParallelReplicas%')
SELECT if((SELECT is_pr FROM has_pr), replaceRegexpOne(explain, '^    ', ''), explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_stats_pruning WHERE value_nullable >= 3000 AND value_nullable <= 3050) WHERE explain NOT LIKE '%MergingAggregated%' AND explain NOT LIKE '%Union%' AND explain NOT LIKE '%ReadFromRemoteParallelReplicas%';
SELECT count() FROM test_stats_pruning WHERE value_nullable >= 3000 AND value_nullable <= 3050;

-- FINAL query should NOT use Statistics pruning
WITH has_pr AS (SELECT count() > 0 AS is_pr FROM (EXPLAIN indexes = 1 SELECT count() FROM test_stats_pruning FINAL WHERE value = 1050) WHERE explain LIKE '%ReadFromRemoteParallelReplicas%')
SELECT if((SELECT is_pr FROM has_pr), replaceRegexpOne(explain, '^    ', ''), explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_stats_pruning FINAL WHERE value = 1050) WHERE explain NOT LIKE '%MergingAggregated%' AND explain NOT LIKE '%Union%' AND explain NOT LIKE '%ReadFromRemoteParallelReplicas%';
SELECT count() FROM test_stats_pruning FINAL WHERE value = 1050;

DROP TABLE test_stats_pruning;
