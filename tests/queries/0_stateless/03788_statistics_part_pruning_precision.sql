-- This test validates that Field-based MinMax statistics can precisely prune parts
-- for large integers (beyond 2^53), high-precision Decimals, and Float64 values.

DROP TABLE IF EXISTS test_stats_exceeds;

CREATE TABLE test_stats_exceeds (
    dt Date,
    val_int64 Int64,
    val_uint64 UInt64,
    val_decimal128 Decimal128(0),
    val_decimal32 Decimal32(0),
    val_decimal32_9 Decimal32(9),
    val_float64 Float64
) ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(dt)
ORDER BY tuple()
SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi', auto_statistics_types = 'minmax';

SET use_statistics_for_part_pruning = 1;
SET enable_analyzer = 1;
SET parallel_replicas_local_plan = 1;
SET optimize_move_to_prewhere = 1;
SET query_plan_optimize_prewhere = 1;

-- =============================================================================
-- Case 1: UInt64 with values >= 2^53
-- Statistics: min=2^53, max=2^53+9
-- Field-based statistics preserve exact UInt64 values, so pruning is precise.
-- =============================================================================
-- Part 1: val_uint64 [2^53, 2^53+9]
INSERT INTO test_stats_exceeds SELECT '2025-01-01', 0, 9007199254740992 + number, toDecimal128(0, 0), toDecimal32(0, 0), toDecimal32(0, 9), 0 FROM numbers(10);

SELECT '-- Case 1: UInt64 >= 2^53, query in range, should NOT prune';
WITH has_pr AS (SELECT count() > 0 AS is_pr FROM (EXPLAIN indexes = 1 SELECT count() FROM test_stats_exceeds WHERE dt = '2025-01-01' AND val_uint64 = 9007199254740997) WHERE explain LIKE '%ReadFromRemoteParallelReplicas%')
SELECT if((SELECT is_pr FROM has_pr), replaceRegexpOne(explain, '^    ', ''), explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_stats_exceeds WHERE dt = '2025-01-01' AND val_uint64 = 9007199254740997) WHERE explain NOT LIKE '%MergingAggregated%' AND explain NOT LIKE '%Union%' AND explain NOT LIKE '%ReadFromRemoteParallelReplicas%';
SELECT count() FROM test_stats_exceeds WHERE dt = '2025-01-01' AND val_uint64 = 9007199254740997;

SELECT '-- Case 1: UInt64 >= 2^53, query out of range, should prune';
WITH has_pr AS (SELECT count() > 0 AS is_pr FROM (EXPLAIN indexes = 1 SELECT count() FROM test_stats_exceeds WHERE dt = '2025-01-01' AND val_uint64 = 50) WHERE explain LIKE '%ReadFromRemoteParallelReplicas%')
SELECT if((SELECT is_pr FROM has_pr), replaceRegexpOne(explain, '^    ', ''), explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_stats_exceeds WHERE dt = '2025-01-01' AND val_uint64 = 50) WHERE explain NOT LIKE '%MergingAggregated%' AND explain NOT LIKE '%Union%' AND explain NOT LIKE '%ReadFromRemoteParallelReplicas%';
SELECT count() FROM test_stats_exceeds WHERE dt = '2025-01-01' AND val_uint64 = 50;


-- =============================================================================
-- Case 2: Int64 spanning normal and large values
-- Statistics: min=100, max=2^53+4
-- =============================================================================
-- Part 2: val_int64 [100, 2^53+4]
INSERT INTO test_stats_exceeds SELECT '2025-01-02', if(number < 5, toInt64(number + 100), toInt64(9007199254740992) + number - 5), 0, toDecimal128(0, 0), toDecimal32(0, 0), toDecimal32(0, 9), 0 FROM numbers(10);

SELECT '-- Case 2: Int64 query < min, should prune';
WITH has_pr AS (SELECT count() > 0 AS is_pr FROM (EXPLAIN indexes = 1 SELECT count() FROM test_stats_exceeds WHERE dt = '2025-01-02' AND val_int64 = 50) WHERE explain LIKE '%ReadFromRemoteParallelReplicas%')
SELECT if((SELECT is_pr FROM has_pr), replaceRegexpOne(explain, '^    ', ''), explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_stats_exceeds WHERE dt = '2025-01-02' AND val_int64 = 50) WHERE explain NOT LIKE '%MergingAggregated%' AND explain NOT LIKE '%Union%' AND explain NOT LIKE '%ReadFromRemoteParallelReplicas%';
SELECT count() FROM test_stats_exceeds WHERE dt = '2025-01-02' AND val_int64 = 50;

SELECT '-- Case 2: Int64 query > max (beyond 2^53), should prune';
WITH has_pr AS (SELECT count() > 0 AS is_pr FROM (EXPLAIN indexes = 1 SELECT count() FROM test_stats_exceeds WHERE dt = '2025-01-02' AND val_int64 = 9007199254740999) WHERE explain LIKE '%ReadFromRemoteParallelReplicas%')
SELECT if((SELECT is_pr FROM has_pr), replaceRegexpOne(explain, '^    ', ''), explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_stats_exceeds WHERE dt = '2025-01-02' AND val_int64 = 9007199254740999) WHERE explain NOT LIKE '%MergingAggregated%' AND explain NOT LIKE '%Union%' AND explain NOT LIKE '%ReadFromRemoteParallelReplicas%';
SELECT count() FROM test_stats_exceeds WHERE dt = '2025-01-02' AND val_int64 = 9007199254740999;

-- =============================================================================
-- Case 3: Int64 with large negative values
-- Statistics: min=-2^53-4, max=-100
-- =============================================================================
-- Part 3: val_int64 [-2^53-4, -100]
INSERT INTO test_stats_exceeds SELECT '2025-01-03', if(number < 5, toInt64(-9007199254740992) - 4 + number, toInt64(-109) + number), 0, toDecimal128(0, 0), toDecimal32(0, 0), toDecimal32(0, 9), 0 FROM numbers(10);

SELECT '-- Case 3: Int64 query > max, should prune';
WITH has_pr AS (SELECT count() > 0 AS is_pr FROM (EXPLAIN indexes = 1 SELECT count() FROM test_stats_exceeds WHERE dt = '2025-01-03' AND val_int64 = -50) WHERE explain LIKE '%ReadFromRemoteParallelReplicas%')
SELECT if((SELECT is_pr FROM has_pr), replaceRegexpOne(explain, '^    ', ''), explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_stats_exceeds WHERE dt = '2025-01-03' AND val_int64 = -50) WHERE explain NOT LIKE '%MergingAggregated%' AND explain NOT LIKE '%Union%' AND explain NOT LIKE '%ReadFromRemoteParallelReplicas%';
SELECT count() FROM test_stats_exceeds WHERE dt = '2025-01-03' AND val_int64 = -50;

SELECT '-- Case 3: Int64 query = max, should NOT prune';
WITH has_pr AS (SELECT count() > 0 AS is_pr FROM (EXPLAIN indexes = 1 SELECT count() FROM test_stats_exceeds WHERE dt = '2025-01-03' AND val_int64 = -100) WHERE explain LIKE '%ReadFromRemoteParallelReplicas%')
SELECT if((SELECT is_pr FROM has_pr), replaceRegexpOne(explain, '^    ', ''), explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_stats_exceeds WHERE dt = '2025-01-03' AND val_int64 = -100) WHERE explain NOT LIKE '%MergingAggregated%' AND explain NOT LIKE '%Union%' AND explain NOT LIKE '%ReadFromRemoteParallelReplicas%';
SELECT count() FROM test_stats_exceeds WHERE dt = '2025-01-03' AND val_int64 = -100;

SELECT '-- Case 3: Int64 query < min (beyond -2^53), should prune';
WITH has_pr AS (SELECT count() > 0 AS is_pr FROM (EXPLAIN indexes = 1 SELECT count() FROM test_stats_exceeds WHERE dt = '2025-01-03' AND val_int64 = -9007199254740999) WHERE explain LIKE '%ReadFromRemoteParallelReplicas%')
SELECT if((SELECT is_pr FROM has_pr), replaceRegexpOne(explain, '^    ', ''), explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_stats_exceeds WHERE dt = '2025-01-03' AND val_int64 = -9007199254740999) WHERE explain NOT LIKE '%MergingAggregated%' AND explain NOT LIKE '%Union%' AND explain NOT LIKE '%ReadFromRemoteParallelReplicas%';
SELECT count() FROM test_stats_exceeds WHERE dt = '2025-01-03' AND val_int64 = -9007199254740999;

-- =============================================================================
-- Case 4: Int64 spanning both extremes
-- Statistics: min=-2^53-4, max=2^53+4
-- Query value is outside the range, so pruning should work.
-- =============================================================================
-- Part 4: val_int64 [-2^53-4, 2^53+4]
INSERT INTO test_stats_exceeds SELECT '2025-01-04', if(number < 5, toInt64(-9007199254740992) - 4 + number, toInt64(9007199254740992) + number - 5), 0, toDecimal128(0, 0), toDecimal32(0, 0), toDecimal32(0, 9), 0 FROM numbers(10);

SELECT '-- Case 4: Int64 extreme range, query outside range, should prune';
WITH has_pr AS (SELECT count() > 0 AS is_pr FROM (EXPLAIN indexes = 1 SELECT count() FROM test_stats_exceeds WHERE dt = '2025-01-04' AND val_int64 = 9007199254740999) WHERE explain LIKE '%ReadFromRemoteParallelReplicas%')
SELECT if((SELECT is_pr FROM has_pr), replaceRegexpOne(explain, '^    ', ''), explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_stats_exceeds WHERE dt = '2025-01-04' AND val_int64 = 9007199254740999) WHERE explain NOT LIKE '%MergingAggregated%' AND explain NOT LIKE '%Union%' AND explain NOT LIKE '%ReadFromRemoteParallelReplicas%';
SELECT count() FROM test_stats_exceeds WHERE dt = '2025-01-04' AND val_int64 = 9007199254740999;

-- =============================================================================
-- Case 5: Decimal128 (precision=38) - now works with Field-based statistics
-- Statistics: min=100, max=200
-- =============================================================================
-- Part 5: val_decimal128 [100, 200]
INSERT INTO test_stats_exceeds SELECT '2025-01-05', 0, 0, toDecimal128(100, 0) + number * 10, toDecimal32(0, 0), toDecimal32(0, 9), 0 FROM numbers(11);

SELECT '-- Case 5: Decimal128, query outside range, should prune';
WITH has_pr AS (SELECT count() > 0 AS is_pr FROM (EXPLAIN indexes = 1 SELECT count() FROM test_stats_exceeds WHERE dt = '2025-01-05' AND val_decimal128 = toDecimal128(50, 0)) WHERE explain LIKE '%ReadFromRemoteParallelReplicas%')
SELECT if((SELECT is_pr FROM has_pr), replaceRegexpOne(explain, '^    ', ''), explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_stats_exceeds WHERE dt = '2025-01-05' AND val_decimal128 = toDecimal128(50, 0)) WHERE explain NOT LIKE '%MergingAggregated%' AND explain NOT LIKE '%Union%' AND explain NOT LIKE '%ReadFromRemoteParallelReplicas%';
SELECT count() FROM test_stats_exceeds WHERE dt = '2025-01-05' AND val_decimal128 = toDecimal128(50, 0);

-- =============================================================================
-- Case 6: Decimal32 extreme values
-- =============================================================================
-- Part 6.1: val_decimal32(0) [900000000, 999999999]
INSERT INTO test_stats_exceeds SELECT '2025-01-06', 0, 0, toDecimal128(0, 0), if(number = 10, toDecimal32(999999999, 0), toDecimal32(900000000, 0) + number * 10000000), toDecimal32(0, 9), 0 FROM numbers(11);

SELECT '-- Case 6.1: Decimal32(0) query at max, should NOT prune';
WITH has_pr AS (SELECT count() > 0 AS is_pr FROM (EXPLAIN indexes = 1 SELECT count() FROM test_stats_exceeds WHERE dt = '2025-01-06' AND val_decimal32 = toDecimal32(999999999, 0)) WHERE explain LIKE '%ReadFromRemoteParallelReplicas%')
SELECT if((SELECT is_pr FROM has_pr), replaceRegexpOne(explain, '^    ', ''), explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_stats_exceeds WHERE dt = '2025-01-06' AND val_decimal32 = toDecimal32(999999999, 0)) WHERE explain NOT LIKE '%MergingAggregated%' AND explain NOT LIKE '%Union%' AND explain NOT LIKE '%ReadFromRemoteParallelReplicas%';
SELECT count() FROM test_stats_exceeds WHERE dt = '2025-01-06' AND val_decimal32 = toDecimal32(999999999, 0);

SELECT '-- Case 6.2: Decimal32(0) query < min, should prune';
WITH has_pr AS (SELECT count() > 0 AS is_pr FROM (EXPLAIN indexes = 1 SELECT count() FROM test_stats_exceeds WHERE dt = '2025-01-06' AND val_decimal32 = toDecimal32(100, 0)) WHERE explain LIKE '%ReadFromRemoteParallelReplicas%')
SELECT if((SELECT is_pr FROM has_pr), replaceRegexpOne(explain, '^    ', ''), explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_stats_exceeds WHERE dt = '2025-01-06' AND val_decimal32 = toDecimal32(100, 0)) WHERE explain NOT LIKE '%MergingAggregated%' AND explain NOT LIKE '%Union%' AND explain NOT LIKE '%ReadFromRemoteParallelReplicas%';
SELECT count() FROM test_stats_exceeds WHERE dt = '2025-01-06' AND val_decimal32 = toDecimal32(100, 0);

-- Part 6.2: val_decimal32_9(9) [0.000000001, 0.999999999]
INSERT INTO test_stats_exceeds SELECT '2025-01-07', 0, 0, toDecimal128(0, 0), toDecimal32(0, 0), if(number = 10, toDecimal32(1, 9) - toDecimal32(0.000000001, 9), toDecimal32(0.000000001, 9) + number * toDecimal32(0.09, 9)), 0 FROM numbers(11);

SELECT '-- Case 6.3: Decimal32(9) query at max, should NOT prune';
WITH has_pr AS (SELECT count() > 0 AS is_pr FROM (EXPLAIN indexes = 1 SELECT count() FROM test_stats_exceeds WHERE dt = '2025-01-07' AND val_decimal32_9 = toDecimal32(0.999999999, 9)) WHERE explain LIKE '%ReadFromRemoteParallelReplicas%')
SELECT if((SELECT is_pr FROM has_pr), replaceRegexpOne(explain, '^    ', ''), explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_stats_exceeds WHERE dt = '2025-01-07' AND val_decimal32_9 = toDecimal32(0.999999999, 9)) WHERE explain NOT LIKE '%MergingAggregated%' AND explain NOT LIKE '%Union%' AND explain NOT LIKE '%ReadFromRemoteParallelReplicas%';
SELECT count() FROM test_stats_exceeds WHERE dt = '2025-01-07' AND val_decimal32_9 = toDecimal32(0.999999999, 9);

-- =============================================================================
-- Case 7: Int64 with all values <= -2^53
-- Statistics: min=-2^53-9, max=-2^53
-- =============================================================================
-- Part 7: val_int64 [-2^53-9, -2^53]
INSERT INTO test_stats_exceeds SELECT '2025-01-08', toInt64(-9007199254740992) - 9 + number, 0, toDecimal128(0, 0), toDecimal32(0, 0), toDecimal32(0, 9), 0 FROM numbers(10);

SELECT '-- Case 7: Int64 <= -2^53, query in range, should NOT prune';
WITH has_pr AS (SELECT count() > 0 AS is_pr FROM (EXPLAIN indexes = 1 SELECT count() FROM test_stats_exceeds WHERE dt = '2025-01-08' AND val_int64 = -9007199254740997) WHERE explain LIKE '%ReadFromRemoteParallelReplicas%')
SELECT if((SELECT is_pr FROM has_pr), replaceRegexpOne(explain, '^    ', ''), explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_stats_exceeds WHERE dt = '2025-01-08' AND val_int64 = -9007199254740997) WHERE explain NOT LIKE '%MergingAggregated%' AND explain NOT LIKE '%Union%' AND explain NOT LIKE '%ReadFromRemoteParallelReplicas%';
SELECT count() FROM test_stats_exceeds WHERE dt = '2025-01-08' AND val_int64 = -9007199254740997;

SELECT '-- Case 7: Int64 <= -2^53, query out of range, should prune';
WITH has_pr AS (SELECT count() > 0 AS is_pr FROM (EXPLAIN indexes = 1 SELECT count() FROM test_stats_exceeds WHERE dt = '2025-01-08' AND val_int64 = -50) WHERE explain LIKE '%ReadFromRemoteParallelReplicas%')
SELECT if((SELECT is_pr FROM has_pr), replaceRegexpOne(explain, '^    ', ''), explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_stats_exceeds WHERE dt = '2025-01-08' AND val_int64 = -50) WHERE explain NOT LIKE '%MergingAggregated%' AND explain NOT LIKE '%Union%' AND explain NOT LIKE '%ReadFromRemoteParallelReplicas%';
SELECT count() FROM test_stats_exceeds WHERE dt = '2025-01-08' AND val_int64 = -50;

-- =============================================================================
-- Case 8: Float64 with large positive values
-- Statistics: min=1000, max=1e17
-- =============================================================================
-- Part 8: val_float64 [1000, 1e17]
INSERT INTO test_stats_exceeds SELECT '2025-01-09', 0, 0, toDecimal128(0, 0), toDecimal32(0, 0), toDecimal32(0, 9), if(number < 5, 1000 + number, 1e17 - (9 - number)) FROM numbers(10);

SELECT '-- Case 8.1: Float64 query < min, should prune';
WITH has_pr AS (SELECT count() > 0 AS is_pr FROM (EXPLAIN indexes = 1 SELECT count() FROM test_stats_exceeds WHERE dt = '2025-01-09' AND val_float64 = 100) WHERE explain LIKE '%ReadFromRemoteParallelReplicas%')
SELECT if((SELECT is_pr FROM has_pr), replaceRegexpOne(explain, '^    ', ''), explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_stats_exceeds WHERE dt = '2025-01-09' AND val_float64 = 100) WHERE explain NOT LIKE '%MergingAggregated%' AND explain NOT LIKE '%Union%' AND explain NOT LIKE '%ReadFromRemoteParallelReplicas%';
SELECT count() FROM test_stats_exceeds WHERE dt = '2025-01-09' AND val_float64 = 100;

SELECT '-- Case 8.2: Float64 query > max, should prune';
WITH has_pr AS (SELECT count() > 0 AS is_pr FROM (EXPLAIN indexes = 1 SELECT count() FROM test_stats_exceeds WHERE dt = '2025-01-09' AND val_float64 = 2e17) WHERE explain LIKE '%ReadFromRemoteParallelReplicas%')
SELECT if((SELECT is_pr FROM has_pr), replaceRegexpOne(explain, '^    ', ''), explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_stats_exceeds WHERE dt = '2025-01-09' AND val_float64 = 2e17) WHERE explain NOT LIKE '%MergingAggregated%' AND explain NOT LIKE '%Union%' AND explain NOT LIKE '%ReadFromRemoteParallelReplicas%';
SELECT count() FROM test_stats_exceeds WHERE dt = '2025-01-09' AND val_float64 = 2e17;

-- =============================================================================
-- Case 9: Float64 with large negative values
-- Statistics: min=-1e17, max=-1000
-- =============================================================================
-- Part 9: val_float64 [-1e17, -1000]
INSERT INTO test_stats_exceeds SELECT '2025-01-10', 0, 0, toDecimal128(0, 0), toDecimal32(0, 0), toDecimal32(0, 9), if(number < 5, -1e17 + number, -1000 - (9 - number)) FROM numbers(10);

SELECT '-- Case 9.1: Float64 query > max, should prune';
WITH has_pr AS (SELECT count() > 0 AS is_pr FROM (EXPLAIN indexes = 1 SELECT count() FROM test_stats_exceeds WHERE dt = '2025-01-10' AND val_float64 = -100) WHERE explain LIKE '%ReadFromRemoteParallelReplicas%')
SELECT if((SELECT is_pr FROM has_pr), replaceRegexpOne(explain, '^    ', ''), explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_stats_exceeds WHERE dt = '2025-01-10' AND val_float64 = -100) WHERE explain NOT LIKE '%MergingAggregated%' AND explain NOT LIKE '%Union%' AND explain NOT LIKE '%ReadFromRemoteParallelReplicas%';
SELECT count() FROM test_stats_exceeds WHERE dt = '2025-01-10' AND val_float64 = -100;

SELECT '-- Case 9.2: Float64 query < min, should prune';
WITH has_pr AS (SELECT count() > 0 AS is_pr FROM (EXPLAIN indexes = 1 SELECT count() FROM test_stats_exceeds WHERE dt = '2025-01-10' AND val_float64 = -2e17) WHERE explain LIKE '%ReadFromRemoteParallelReplicas%')
SELECT if((SELECT is_pr FROM has_pr), replaceRegexpOne(explain, '^    ', ''), explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_stats_exceeds WHERE dt = '2025-01-10' AND val_float64 = -2e17) WHERE explain NOT LIKE '%MergingAggregated%' AND explain NOT LIKE '%Union%' AND explain NOT LIKE '%ReadFromRemoteParallelReplicas%';
SELECT count() FROM test_stats_exceeds WHERE dt = '2025-01-10' AND val_float64 = -2e17;

DROP TABLE test_stats_exceeds;
