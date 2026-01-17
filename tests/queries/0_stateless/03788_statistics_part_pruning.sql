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
SETTINGS min_bytes_for_wide_part = 0, auto_statistics_types = 'minmax';

SET use_statistics_part_pruning = 1;

-- Part 1: 202501, id [0, 99], value [0, 99], version [0, 99]
INSERT INTO test_stats_pruning SELECT '2025-01-11', number, number, number, toDecimal128(number, 0), 'a', number FROM numbers(100);
-- Part 2: 202501, id [100, 199], value [1000, 1099], version [1000, 1099]
INSERT INTO test_stats_pruning SELECT '2025-01-12', number + 100, number + 1000, number + 1000, toDecimal128(number + 1000, 0), 'b', number + 1000 FROM numbers(100);
-- Part 3: 202501, id [200, 299], value [2000, 2099], value_nullable NULL, version [2000, 2099]
INSERT INTO test_stats_pruning SELECT '2025-01-13', number + 200, number + 2000, NULL, toDecimal128(number + 2000, 0), 'c', number + 2000 FROM numbers(100);
-- Part 4: 202501, id [300, 399], value [3000, 3099], version [3000, 3099]
INSERT INTO test_stats_pruning SELECT '2025-01-14', number + 300, number + 3000, number + 3000, toDecimal128(number + 3000, 0), 'd', number + 3000 FROM numbers(100);

SELECT '-- AND on different columns all have statistics, should prune parts';
WITH has_pr AS (SELECT count() > 0 AS is_pr FROM (EXPLAIN indexes = 1 SELECT count() FROM test_stats_pruning WHERE id >= 50 AND NOT(value > 500) AND version IN (50, 1050)) WHERE explain LIKE '%ReadFromRemoteParallelReplicas%')
SELECT if((SELECT is_pr FROM has_pr), replaceRegexpOne(explain, '^    ', ''), explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_stats_pruning WHERE id >= 50 AND NOT(value > 500) AND version IN (50, 1050)) WHERE explain NOT LIKE '%MergingAggregated%' AND explain NOT LIKE '%Union%' AND explain NOT LIKE '%ReadFromRemoteParallelReplicas%';
SELECT count() FROM test_stats_pruning WHERE id >= 50 AND NOT(value > 500) AND version IN (50, 1050);

SELECT '-- OR on different columns all have statistics, should prune parts';
WITH has_pr AS (SELECT count() > 0 AS is_pr FROM (EXPLAIN indexes = 1 SELECT count() FROM test_stats_pruning WHERE id < 50 OR version > 3050) WHERE explain LIKE '%ReadFromRemoteParallelReplicas%')
SELECT if((SELECT is_pr FROM has_pr), replaceRegexpOne(explain, '^    ', ''), explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_stats_pruning WHERE id < 50 OR version > 3050) WHERE explain NOT LIKE '%MergingAggregated%' AND explain NOT LIKE '%Union%' AND explain NOT LIKE '%ReadFromRemoteParallelReplicas%';
SELECT count() FROM test_stats_pruning WHERE id < 50 OR version > 3050;

SELECT '-- AND with column without statistics (str has no minmax), should prune parts';
WITH has_pr AS (SELECT count() > 0 AS is_pr FROM (EXPLAIN indexes = 1 SELECT count() FROM test_stats_pruning WHERE value = 50 AND str = 'a') WHERE explain LIKE '%ReadFromRemoteParallelReplicas%')
SELECT if((SELECT is_pr FROM has_pr), replaceRegexpOne(explain, '^    ', ''), explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_stats_pruning WHERE value = 50 AND str = 'a') WHERE explain NOT LIKE '%MergingAggregated%' AND explain NOT LIKE '%Union%' AND explain NOT LIKE '%ReadFromRemoteParallelReplicas%';
SELECT count() FROM test_stats_pruning WHERE value = 50 AND str = 'a';

SELECT '-- OR with column without statistics (str has no minmax), should NOT prune parts';
WITH has_pr AS (SELECT count() > 0 AS is_pr FROM (EXPLAIN indexes = 1 SELECT count() FROM test_stats_pruning WHERE value = 50 OR str = 'x') WHERE explain LIKE '%ReadFromRemoteParallelReplicas%')
SELECT if((SELECT is_pr FROM has_pr), replaceRegexpOne(explain, '^    ', ''), explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_stats_pruning WHERE value = 50 OR str = 'x') WHERE explain NOT LIKE '%MergingAggregated%' AND explain NOT LIKE '%Union%' AND explain NOT LIKE '%ReadFromRemoteParallelReplicas%';
SELECT count() FROM test_stats_pruning WHERE value = 50 OR str = 'x';

SELECT '-- partition and statistics pruning';
WITH has_pr AS (SELECT count() > 0 AS is_pr FROM (EXPLAIN indexes = 1 SELECT count() FROM test_stats_pruning WHERE dt = '2025-01-11' AND value = 1000) WHERE explain LIKE '%ReadFromRemoteParallelReplicas%')
SELECT if((SELECT is_pr FROM has_pr), replaceRegexpOne(explain, '^    ', ''), explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_stats_pruning WHERE dt = '2025-01-11' AND value = 1000) WHERE explain NOT LIKE '%MergingAggregated%' AND explain NOT LIKE '%Union%' AND explain NOT LIKE '%ReadFromRemoteParallelReplicas%';
SELECT count() FROM test_stats_pruning WHERE dt = '2025-01-11' AND value = 1000;

SELECT '-- Nullable column pruning, Part 3 has NULL values, should be pruned';
WITH has_pr AS (SELECT count() > 0 AS is_pr FROM (EXPLAIN indexes = 1 SELECT count() FROM test_stats_pruning WHERE value_nullable >= 3000 AND value_nullable <= 3050) WHERE explain LIKE '%ReadFromRemoteParallelReplicas%')
SELECT if((SELECT is_pr FROM has_pr), replaceRegexpOne(explain, '^    ', ''), explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_stats_pruning WHERE value_nullable >= 3000 AND value_nullable <= 3050) WHERE explain NOT LIKE '%MergingAggregated%' AND explain NOT LIKE '%Union%' AND explain NOT LIKE '%ReadFromRemoteParallelReplicas%';
SELECT count() FROM test_stats_pruning WHERE value_nullable >= 3000 AND value_nullable <= 3050;

SELECT '-- FINAL query should NOT use Statistics pruning';
WITH has_pr AS (SELECT count() > 0 AS is_pr FROM (EXPLAIN indexes = 1 SELECT count() FROM test_stats_pruning FINAL WHERE value = 1050) WHERE explain LIKE '%ReadFromRemoteParallelReplicas%')
SELECT if((SELECT is_pr FROM has_pr), replaceRegexpOne(explain, '^    ', ''), explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_stats_pruning FINAL WHERE value = 1050) WHERE explain NOT LIKE '%MergingAggregated%' AND explain NOT LIKE '%Union%' AND explain NOT LIKE '%ReadFromRemoteParallelReplicas%';
SELECT count() FROM test_stats_pruning FINAL WHERE value = 1050;

SELECT '-- Non-monotonic function wrapping column length(toString(value)), should NOT use Statistics pruning';
WITH has_pr AS (SELECT count() > 0 AS is_pr FROM (EXPLAIN indexes = 1 SELECT count() FROM test_stats_pruning WHERE length(toString(value)) = 2) WHERE explain LIKE '%ReadFromRemoteParallelReplicas%')
SELECT if((SELECT is_pr FROM has_pr), replaceRegexpOne(explain, '^    ', ''), explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_stats_pruning WHERE length(toString(value)) = 2) WHERE explain NOT LIKE '%MergingAggregated%' AND explain NOT LIKE '%Union%' AND explain NOT LIKE '%ReadFromRemoteParallelReplicas%';
SELECT count() FROM test_stats_pruning WHERE length(toString(value)) = 2;

DROP TABLE test_stats_pruning;
