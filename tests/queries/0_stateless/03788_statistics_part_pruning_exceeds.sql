-- This test specifically validates precision-loss handling logic for Int64, UInt64, and Decimal types.

DROP TABLE IF EXISTS test_stats_exceeds;

CREATE TABLE test_stats_exceeds (
    dt Date,
    val_int64 Int64,
    val_uint64 UInt64,
    val_decimal128 Decimal128(0),   -- precision = 38 > 15, skips pruning entirely
    val_decimal32 Decimal32(0),      -- precision = 9 <= 15, can prune normally
    val_decimal32_9 Decimal32(9),      -- precision = 9 <= 15, can prune normally
) ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(dt)
ORDER BY tuple()
SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi', auto_statistics_types = 'minmax';

SET use_statistics_for_part_pruning = 1;
SET enable_analyzer = 1;
SET parallel_replicas_local_plan = 1;

-- =============================================================================
-- Case 1: UInt64 with min >= 2^53, both min and max exceed precision
-- Statistics: min=2^53, max=2^53+9
-- Expected: detects precision loss, uses [2^53, +inf)
-- =============================================================================
-- Part 1: val_uint64 [2^53, 2^53+9]
INSERT INTO test_stats_exceeds SELECT '2025-01-01', 0, 9007199254740992 + number, toDecimal128(0, 0), toDecimal32(0, 0), toDecimal32(0, 9) FROM numbers(10);

SELECT '-- Case 1: UInt64 both >= 2^53, query value = 2^53+5, should NOT prune part 1';
WITH has_pr AS (SELECT count() > 0 AS is_pr FROM (EXPLAIN indexes = 1 SELECT count() FROM test_stats_exceeds WHERE dt = '2025-01-01' AND val_uint64 = 9007199254740997) WHERE explain LIKE '%ReadFromRemoteParallelReplicas%')
SELECT if((SELECT is_pr FROM has_pr), replaceRegexpOne(explain, '^    ', ''), explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_stats_exceeds WHERE dt = '2025-01-01' AND val_uint64 = 9007199254740997) WHERE explain NOT LIKE '%MergingAggregated%' AND explain NOT LIKE '%Union%' AND explain NOT LIKE '%ReadFromRemoteParallelReplicas%';
SELECT count() FROM test_stats_exceeds WHERE dt = '2025-01-01' AND val_uint64 = 9007199254740997;

SELECT '-- Case 1: UInt64 both >= 2^53, query value < 2^53, should prune part 1';
WITH has_pr AS (SELECT count() > 0 AS is_pr FROM (EXPLAIN indexes = 1 SELECT count() FROM test_stats_exceeds WHERE dt = '2025-01-01' AND val_uint64 = 50) WHERE explain LIKE '%ReadFromRemoteParallelReplicas%')
SELECT if((SELECT is_pr FROM has_pr), replaceRegexpOne(explain, '^    ', ''), explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_stats_exceeds WHERE dt = '2025-01-01' AND val_uint64 = 50) WHERE explain NOT LIKE '%MergingAggregated%' AND explain NOT LIKE '%Union%' AND explain NOT LIKE '%ReadFromRemoteParallelReplicas%';
SELECT count() FROM test_stats_exceeds WHERE dt = '2025-01-01' AND val_uint64 = 50;


-- =============================================================================
-- Case 2: Int64 with min in range, max >= 2^53
-- Statistics: min=100, max=2^53+4
-- Expected: min converts OK, max fails -> uses [100, +inf)
-- =============================================================================
-- Part 2: val_int64 [100, 2^53+4]
INSERT INTO test_stats_exceeds SELECT '2025-01-02', if(number < 5, toInt64(number + 100), toInt64(9007199254740992) + number - 5), 0, toDecimal128(0, 0), toDecimal32(0, 0), toDecimal32(0, 9) FROM numbers(10);

SELECT '-- Case 2: Int64 min in range, max >= 2^53, query < min, should prune part 2';
WITH has_pr AS (SELECT count() > 0 AS is_pr FROM (EXPLAIN indexes = 1 SELECT count() FROM test_stats_exceeds WHERE dt = '2025-01-02' AND val_int64 = 50) WHERE explain LIKE '%ReadFromRemoteParallelReplicas%')
SELECT if((SELECT is_pr FROM has_pr), replaceRegexpOne(explain, '^    ', ''), explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_stats_exceeds WHERE dt = '2025-01-02' AND val_int64 = 50) WHERE explain NOT LIKE '%MergingAggregated%' AND explain NOT LIKE '%Union%' AND explain NOT LIKE '%ReadFromRemoteParallelReplicas%';
SELECT count() FROM test_stats_exceeds WHERE dt = '2025-01-02' AND val_int64 = 50;

SELECT '-- Case 2: Int64 min in range, max >= 2^53, query > 2^53, should NOT prune part 2';
WITH has_pr AS (SELECT count() > 0 AS is_pr FROM (EXPLAIN indexes = 1 SELECT count() FROM test_stats_exceeds WHERE dt = '2025-01-02' AND val_int64 = 9007199254740993) WHERE explain LIKE '%ReadFromRemoteParallelReplicas%')
SELECT if((SELECT is_pr FROM has_pr), replaceRegexpOne(explain, '^    ', ''), explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_stats_exceeds WHERE dt = '2025-01-02' AND val_int64 = 9007199254740993) WHERE explain NOT LIKE '%MergingAggregated%' AND explain NOT LIKE '%Union%' AND explain NOT LIKE '%ReadFromRemoteParallelReplicas%';
SELECT count() FROM test_stats_exceeds WHERE dt = '2025-01-02' AND val_int64 = 9007199254740993;

-- =============================================================================
-- Case 3: Int64 with min <= -2^53, max in range
-- Statistics: min=-2^53-4, max=-100
-- Expected: min fails, max converts OK -> uses (-inf, -100]
-- =============================================================================
-- Part 3: val_int64 [-2^53-4, -100]
INSERT INTO test_stats_exceeds SELECT '2025-01-03', if(number < 5, toInt64(-9007199254740992) - 4 + number, toInt64(-109) + number), 0, toDecimal128(0, 0), toDecimal32(0, 0), toDecimal32(0, 9) FROM numbers(10);

SELECT '-- Case 3: Int64 min <= -2^53, max in range, query > max, should prune part 3';
WITH has_pr AS (SELECT count() > 0 AS is_pr FROM (EXPLAIN indexes = 1 SELECT count() FROM test_stats_exceeds WHERE dt = '2025-01-03' AND val_int64 = -50) WHERE explain LIKE '%ReadFromRemoteParallelReplicas%')
SELECT if((SELECT is_pr FROM has_pr), replaceRegexpOne(explain, '^    ', ''), explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_stats_exceeds WHERE dt = '2025-01-03' AND val_int64 = -50) WHERE explain NOT LIKE '%MergingAggregated%' AND explain NOT LIKE '%Union%' AND explain NOT LIKE '%ReadFromRemoteParallelReplicas%';
SELECT count() FROM test_stats_exceeds WHERE dt = '2025-01-03' AND val_int64 = -50;

SELECT '-- Case 3: Int64 min <= -2^53, max in range, query = max, should NOT prune part 3';
WITH has_pr AS (SELECT count() > 0 AS is_pr FROM (EXPLAIN indexes = 1 SELECT count() FROM test_stats_exceeds WHERE dt = '2025-01-03' AND val_int64 = -100) WHERE explain LIKE '%ReadFromRemoteParallelReplicas%')
SELECT if((SELECT is_pr FROM has_pr), replaceRegexpOne(explain, '^    ', ''), explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_stats_exceeds WHERE dt = '2025-01-03' AND val_int64 = -100) WHERE explain NOT LIKE '%MergingAggregated%' AND explain NOT LIKE '%Union%' AND explain NOT LIKE '%ReadFromRemoteParallelReplicas%';
SELECT count() FROM test_stats_exceeds WHERE dt = '2025-01-03' AND val_int64 = -100;

SELECT '-- Case 3: Int64 min <= -2^53, max in range, query < -2^53, should NOT prune part 3';
WITH has_pr AS (SELECT count() > 0 AS is_pr FROM (EXPLAIN indexes = 1 SELECT count() FROM test_stats_exceeds WHERE dt = '2025-01-03' AND val_int64 = -9007199254740995) WHERE explain LIKE '%ReadFromRemoteParallelReplicas%')
SELECT if((SELECT is_pr FROM has_pr), replaceRegexpOne(explain, '^    ', ''), explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_stats_exceeds WHERE dt = '2025-01-03' AND val_int64 = -9007199254740995) WHERE explain NOT LIKE '%MergingAggregated%' AND explain NOT LIKE '%Union%' AND explain NOT LIKE '%ReadFromRemoteParallelReplicas%';
SELECT count() FROM test_stats_exceeds WHERE dt = '2025-01-03' AND val_int64 = -9007199254740995;

-- =============================================================================
-- Case 4: Int64 with min <= -2^53 and max >= 2^53
-- Statistics: min=-2^53-4, max=2^53+4
-- Expected: Both fail conversion -> uses whole universe, no pruning
-- =============================================================================
-- Part 4: val_int64 [-2^53-4, 2^53+4]
INSERT INTO test_stats_exceeds SELECT '2025-01-04', if(number < 5, toInt64(-9007199254740992) - 4 + number, toInt64(9007199254740992) + number - 5), 0, toDecimal128(0, 0), toDecimal32(0, 0), toDecimal32(0, 9) FROM numbers(10);

SELECT '-- Case 4: Int64 min <= -2^53 and max >= 2^53, cannot prune';
WITH has_pr AS (SELECT count() > 0 AS is_pr FROM (EXPLAIN indexes = 1 SELECT count() FROM test_stats_exceeds WHERE dt = '2025-01-04' AND val_int64 = 9007199254740995) WHERE explain LIKE '%ReadFromRemoteParallelReplicas%')
SELECT if((SELECT is_pr FROM has_pr), replaceRegexpOne(explain, '^    ', ''), explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_stats_exceeds WHERE dt = '2025-01-04' AND val_int64 = 9007199254740995) WHERE explain NOT LIKE '%MergingAggregated%' AND explain NOT LIKE '%Union%' AND explain NOT LIKE '%ReadFromRemoteParallelReplicas%';
SELECT count() FROM test_stats_exceeds WHERE dt = '2025-01-04' AND val_int64 = 9007199254740995;

-- =============================================================================
-- Case 5: Decimal128 (precision=38 > 15) - skips pruning entirely
-- Statistics: min=100, max=200
-- Expected: Decimal128 precision > 15 -> skip statistics pruning, no prune
-- =============================================================================
-- Part 5: val_decimal128 [100, 200]
INSERT INTO test_stats_exceeds SELECT '2025-01-05', 0, 0, toDecimal128(100, 0) + number * 10, toDecimal32(0, 0), toDecimal32(0, 9) FROM numbers(11);

SELECT '-- Case 5: Decimal128 precision=38 > 15, should NOT prune (skips statistics)';
WITH has_pr AS (SELECT count() > 0 AS is_pr FROM (EXPLAIN indexes = 1 SELECT count() FROM test_stats_exceeds WHERE dt = '2025-01-05' AND val_decimal128 = toDecimal128(50, 0)) WHERE explain LIKE '%ReadFromRemoteParallelReplicas%')
SELECT if((SELECT is_pr FROM has_pr), replaceRegexpOne(explain, '^    ', ''), explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_stats_exceeds WHERE dt = '2025-01-05' AND val_decimal128 = toDecimal128(50, 0)) WHERE explain NOT LIKE '%MergingAggregated%' AND explain NOT LIKE '%Union%' AND explain NOT LIKE '%ReadFromRemoteParallelReplicas%';
SELECT count() FROM test_stats_exceeds WHERE dt = '2025-01-05' AND val_decimal128 = toDecimal128(50, 0);

-- =============================================================================
-- Case 6: Decimal32 (precision=9 <= 15) - can prune normally
-- Expected: Decimal32 precision <= 15 -> normal pruning works even at extreme values
-- =============================================================================
-- Part 6.1: val_decimal32(0) [900000000, 999999999]
INSERT INTO test_stats_exceeds SELECT '2025-01-06', 0, 0, toDecimal128(0, 0), if(number = 10, toDecimal32(999999999, 0), toDecimal32(900000000, 0) + number * 10000000), toDecimal32(0, 9) FROM numbers(11);

SELECT '-- Case 6.1: Decimal32(0) precision=9 <= 15 with extreme value, query at max limit 999999999, should NOT prune part 6';
WITH has_pr AS (SELECT count() > 0 AS is_pr FROM (EXPLAIN indexes = 1 SELECT count() FROM test_stats_exceeds WHERE dt = '2025-01-06' AND val_decimal32 = toDecimal32(999999999, 0)) WHERE explain LIKE '%ReadFromRemoteParallelReplicas%')
SELECT if((SELECT is_pr FROM has_pr), replaceRegexpOne(explain, '^    ', ''), explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_stats_exceeds WHERE dt = '2025-01-06' AND val_decimal32 = toDecimal32(999999999, 0)) WHERE explain NOT LIKE '%MergingAggregated%' AND explain NOT LIKE '%Union%' AND explain NOT LIKE '%ReadFromRemoteParallelReplicas%';
SELECT count() FROM test_stats_exceeds WHERE dt = '2025-01-06' AND val_decimal32 = toDecimal32(999999999, 0);

SELECT '-- Case 6.2: Decimal32(0) precision=9 <= 15 with extreme value, query < min, should prune part 6';
WITH has_pr AS (SELECT count() > 0 AS is_pr FROM (EXPLAIN indexes = 1 SELECT count() FROM test_stats_exceeds WHERE dt = '2025-01-06' AND val_decimal32 = toDecimal32(100, 0)) WHERE explain LIKE '%ReadFromRemoteParallelReplicas%')
SELECT if((SELECT is_pr FROM has_pr), replaceRegexpOne(explain, '^    ', ''), explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_stats_exceeds WHERE dt = '2025-01-06' AND val_decimal32 = toDecimal32(100, 0)) WHERE explain NOT LIKE '%MergingAggregated%' AND explain NOT LIKE '%Union%' AND explain NOT LIKE '%ReadFromRemoteParallelReplicas%';
SELECT count() FROM test_stats_exceeds WHERE dt = '2025-01-06' AND val_decimal32 = toDecimal32(100, 0);

-- Part 6.2: val_decimal32_9(9) [0.000000001, 0.999999999]
INSERT INTO test_stats_exceeds SELECT '2025-01-07', 0, 0, toDecimal128(0, 0), toDecimal32(0, 0), if(number = 10, toDecimal32(1, 9) - toDecimal32(0.000000001, 9), toDecimal32(0.000000001, 9) + number * toDecimal32(0.09, 9)) FROM numbers(11);

SELECT '-- Case 6.3: Decimal32(9) precision=9 <= 15 with extreme value, query at max limit 0.999999999, should NOT prune part 6';
WITH has_pr AS (SELECT count() > 0 AS is_pr FROM (EXPLAIN indexes = 1 SELECT count() FROM test_stats_exceeds WHERE dt = '2025-01-07' AND val_decimal32_9 = toDecimal32(0.999999999, 9)) WHERE explain LIKE '%ReadFromRemoteParallelReplicas%')
SELECT if((SELECT is_pr FROM has_pr), replaceRegexpOne(explain, '^    ', ''), explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_stats_exceeds WHERE dt = '2025-01-07' AND val_decimal32_9 = toDecimal32(0.999999999, 9)) WHERE explain NOT LIKE '%MergingAggregated%' AND explain NOT LIKE '%Union%' AND explain NOT LIKE '%ReadFromRemoteParallelReplicas%';
SELECT count() FROM test_stats_exceeds WHERE dt = '2025-01-07' AND val_decimal32_9 = toDecimal32(0.999999999, 9);

-- =============================================================================
-- Case 7: Int64 with max <= -2^53, both min and max in negative overflow region
-- Statistics: min=-2^53-9, max=-2^53
-- Expected: detects precision loss, uses (-inf, -2^53]
-- =============================================================================
-- Part 7: val_int64 [-2^53-9, -2^53]
INSERT INTO test_stats_exceeds SELECT '2025-01-08', toInt64(-9007199254740992) - 9 + number, 0, toDecimal128(0, 0), toDecimal32(0, 0), toDecimal32(0, 9) FROM numbers(10);

SELECT '-- Case 7: Int64 both <= -2^53, query value = -2^53-5, should NOT prune part 7';
WITH has_pr AS (SELECT count() > 0 AS is_pr FROM (EXPLAIN indexes = 1 SELECT count() FROM test_stats_exceeds WHERE dt = '2025-01-08' AND val_int64 = -9007199254740997) WHERE explain LIKE '%ReadFromRemoteParallelReplicas%')
SELECT if((SELECT is_pr FROM has_pr), replaceRegexpOne(explain, '^    ', ''), explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_stats_exceeds WHERE dt = '2025-01-08' AND val_int64 = -9007199254740997) WHERE explain NOT LIKE '%MergingAggregated%' AND explain NOT LIKE '%Union%' AND explain NOT LIKE '%ReadFromRemoteParallelReplicas%';
SELECT count() FROM test_stats_exceeds WHERE dt = '2025-01-08' AND val_int64 = -9007199254740997;

SELECT '-- Case 7: Int64 both <= -2^53, query value > -2^53, should prune part 7';
WITH has_pr AS (SELECT count() > 0 AS is_pr FROM (EXPLAIN indexes = 1 SELECT count() FROM test_stats_exceeds WHERE dt = '2025-01-08' AND val_int64 = -50) WHERE explain LIKE '%ReadFromRemoteParallelReplicas%')
SELECT if((SELECT is_pr FROM has_pr), replaceRegexpOne(explain, '^    ', ''), explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_stats_exceeds WHERE dt = '2025-01-08' AND val_int64 = -50) WHERE explain NOT LIKE '%MergingAggregated%' AND explain NOT LIKE '%Union%' AND explain NOT LIKE '%ReadFromRemoteParallelReplicas%';
SELECT count() FROM test_stats_exceeds WHERE dt = '2025-01-08' AND val_int64 = -50;

DROP TABLE test_stats_exceeds;
