-- This test specifically validates the precision-loss handling logic for Int64, UInt64, and Decimal types
-- when statistics min/max values exceed Float64 precision range [-2^53, 2^53].

DROP TABLE IF EXISTS test_stats_exceeds;

CREATE TABLE test_stats_exceeds (
    dt Date,
    id UInt64,
    val_int64 Int64,
    val_uint64 UInt64,
    val_decimal Decimal128(0)
) ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(dt)
ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, auto_statistics_types = 'minmax';

SET use_statistics_part_pruning = 1;

-- =============================================================================
-- Case 1: UInt64 with min >= 2^53, both min and max exceed precision
-- Statistics: min=2^53, max=2^53+9, stored as Float64 loses precision
-- Expected: Use boundary [2^53, +inf), can prune if query < 2^53
-- =============================================================================
-- Part 1: val_uint64 [2^53, 2^53+9]
INSERT INTO test_stats_exceeds SELECT '2025-01-01', number, 0, 9007199254740992 + number, toDecimal128(0, 0) FROM numbers(10);

SELECT '-- Case 1: UInt64 both >= 2^53, query value < 2^53, should prune part 1';
WITH has_pr AS (SELECT count() > 0 AS is_pr FROM (EXPLAIN indexes = 1 SELECT count() FROM test_stats_exceeds WHERE val_uint64 = 100) WHERE explain LIKE '%ReadFromRemoteParallelReplicas%')
SELECT if((SELECT is_pr FROM has_pr), replaceRegexpOne(explain, '^    ', ''), explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_stats_exceeds WHERE val_uint64 = 100) WHERE explain NOT LIKE '%MergingAggregated%' AND explain NOT LIKE '%Union%' AND explain NOT LIKE '%ReadFromRemoteParallelReplicas%';
SELECT count() FROM test_stats_exceeds WHERE val_uint64 = 100;

SELECT '-- Case 1: UInt64 both >= 2^53, query value = 2^53+5, should NOT prune part 1';
WITH has_pr AS (SELECT count() > 0 AS is_pr FROM (EXPLAIN indexes = 1 SELECT count() FROM test_stats_exceeds WHERE val_uint64 = 9007199254740997) WHERE explain LIKE '%ReadFromRemoteParallelReplicas%')
SELECT if((SELECT is_pr FROM has_pr), replaceRegexpOne(explain, '^    ', ''), explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_stats_exceeds WHERE val_uint64 = 9007199254740997) WHERE explain NOT LIKE '%MergingAggregated%' AND explain NOT LIKE '%Union%' AND explain NOT LIKE '%ReadFromRemoteParallelReplicas%';
SELECT count() FROM test_stats_exceeds WHERE val_uint64 = 9007199254740997;

-- =============================================================================
-- Case 2: Int64 with max <= -2^53, both min and max are negative large
-- Statistics: min=-2^53-9, max=-2^53, stored as Float64 loses precision
-- Expected: Use boundary (-inf, -2^53], can prune if query > -2^53
-- =============================================================================
-- Part 2: val_int64 [-2^53-9, -2^53]
INSERT INTO test_stats_exceeds SELECT '2025-01-02', number, -9007199254740992 - 9 + number, 0, toDecimal128(0, 0) FROM numbers(10);

SELECT '-- Case 2: Int64 both <= -2^53, query value > -2^53, should prune part 2';
WITH has_pr AS (SELECT count() > 0 AS is_pr FROM (EXPLAIN indexes = 1 SELECT count() FROM test_stats_exceeds WHERE val_int64 = -100) WHERE explain LIKE '%ReadFromRemoteParallelReplicas%')
SELECT if((SELECT is_pr FROM has_pr), replaceRegexpOne(explain, '^    ', ''), explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_stats_exceeds WHERE val_int64 = -100) WHERE explain NOT LIKE '%MergingAggregated%' AND explain NOT LIKE '%Union%' AND explain NOT LIKE '%ReadFromRemoteParallelReplicas%';
SELECT count() FROM test_stats_exceeds WHERE val_int64 = -100;

SELECT '-- Case 2: Int64 both <= -2^53, query value = -2^53-5, should NOT prune part 2';
WITH has_pr AS (SELECT count() > 0 AS is_pr FROM (EXPLAIN indexes = 1 SELECT count() FROM test_stats_exceeds WHERE val_int64 = -9007199254740997) WHERE explain LIKE '%ReadFromRemoteParallelReplicas%')
SELECT if((SELECT is_pr FROM has_pr), replaceRegexpOne(explain, '^    ', ''), explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_stats_exceeds WHERE val_int64 = -9007199254740997) WHERE explain NOT LIKE '%MergingAggregated%' AND explain NOT LIKE '%Union%' AND explain NOT LIKE '%ReadFromRemoteParallelReplicas%';
SELECT count() FROM test_stats_exceeds WHERE val_int64 = -9007199254740997;

-- =============================================================================
-- Case 3: Int64 with min in range, max >= 2^53
-- Statistics: min=100, max=2^53+4
-- Expected: Use [100, +inf), can prune if query < 100
-- =============================================================================
-- Part 3: val_int64 [100, 2^53+4]
INSERT INTO test_stats_exceeds SELECT '2025-01-03', number, if(number < 5, toInt64(number + 100), toInt64(9007199254740992) + number - 5), 0, toDecimal128(0, 0) FROM numbers(10);

SELECT '-- Case 3: Int64 min in range, max >= 2^53, query < min, should prune part 3';
WITH has_pr AS (SELECT count() > 0 AS is_pr FROM (EXPLAIN indexes = 1 SELECT count() FROM test_stats_exceeds WHERE val_int64 = 50) WHERE explain LIKE '%ReadFromRemoteParallelReplicas%')
SELECT if((SELECT is_pr FROM has_pr), replaceRegexpOne(explain, '^    ', ''), explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_stats_exceeds WHERE val_int64 = 50) WHERE explain NOT LIKE '%MergingAggregated%' AND explain NOT LIKE '%Union%' AND explain NOT LIKE '%ReadFromRemoteParallelReplicas%';
SELECT count() FROM test_stats_exceeds WHERE val_int64 = 50;

SELECT '-- Case 3: Int64 min in range, max >= 2^53, query > 2^53, should NOT prune part 3';
WITH has_pr AS (SELECT count() > 0 AS is_pr FROM (EXPLAIN indexes = 1 SELECT count() FROM test_stats_exceeds WHERE val_int64 = 9007199254740993) WHERE explain LIKE '%ReadFromRemoteParallelReplicas%')
SELECT if((SELECT is_pr FROM has_pr), replaceRegexpOne(explain, '^    ', ''), explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_stats_exceeds WHERE val_int64 = 9007199254740993) WHERE explain NOT LIKE '%MergingAggregated%' AND explain NOT LIKE '%Union%' AND explain NOT LIKE '%ReadFromRemoteParallelReplicas%';
SELECT count() FROM test_stats_exceeds WHERE val_int64 = 9007199254740993;

-- =============================================================================
-- Case 4: Int64 with min <= -2^53, max in range
-- Statistics: min=-2^53-4, max=-100
-- Expected: Use (-inf, -100], can prune if query > -100
-- =============================================================================
-- Part 4: val_int64 [-2^53-4, -100]
INSERT INTO test_stats_exceeds SELECT '2025-01-04', number, if(number < 5, toInt64(-9007199254740992) - 4 + number, toInt64(-104) + number), 0, toDecimal128(0, 0) FROM numbers(10);

SELECT '-- Case 4: Int64 min <= -2^53, max in range, query > max, should prune part 4';
WITH has_pr AS (SELECT count() > 0 AS is_pr FROM (EXPLAIN indexes = 1 SELECT count() FROM test_stats_exceeds WHERE val_int64 = -50) WHERE explain LIKE '%ReadFromRemoteParallelReplicas%')
SELECT if((SELECT is_pr FROM has_pr), replaceRegexpOne(explain, '^    ', ''), explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_stats_exceeds WHERE val_int64 = -50) WHERE explain NOT LIKE '%MergingAggregated%' AND explain NOT LIKE '%Union%' AND explain NOT LIKE '%ReadFromRemoteParallelReplicas%';
SELECT count() FROM test_stats_exceeds WHERE val_int64 = -50;

SELECT '-- Case 4: Int64 min <= -2^53, max in range, query = max, should NOT prune part 4';
WITH has_pr AS (SELECT count() > 0 AS is_pr FROM (EXPLAIN indexes = 1 SELECT count() FROM test_stats_exceeds WHERE val_int64 = -100) WHERE explain LIKE '%ReadFromRemoteParallelReplicas%')
SELECT if((SELECT is_pr FROM has_pr), replaceRegexpOne(explain, '^    ', ''), explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_stats_exceeds WHERE val_int64 = -100) WHERE explain NOT LIKE '%MergingAggregated%' AND explain NOT LIKE '%Union%' AND explain NOT LIKE '%ReadFromRemoteParallelReplicas%';
SELECT count() FROM test_stats_exceeds WHERE val_int64 = -100;

SELECT '-- Case 4: Int64 min <= -2^53, max in range, query < -2^53, should NOT prune part 4';
WITH has_pr AS (SELECT count() > 0 AS is_pr FROM (EXPLAIN indexes = 1 SELECT count() FROM test_stats_exceeds WHERE val_int64 = -9007199254740995) WHERE explain LIKE '%ReadFromRemoteParallelReplicas%')
SELECT if((SELECT is_pr FROM has_pr), replaceRegexpOne(explain, '^    ', ''), explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_stats_exceeds WHERE val_int64 = -9007199254740995) WHERE explain NOT LIKE '%MergingAggregated%' AND explain NOT LIKE '%Union%' AND explain NOT LIKE '%ReadFromRemoteParallelReplicas%';
SELECT count() FROM test_stats_exceeds WHERE val_int64 = -9007199254740995;

-- =============================================================================
-- Case 5: Int64 with min <= -2^53 and max >= 2^53
-- Statistics: min=-2^53-4, max=2^53+4
-- Expected: Cannot determine any bound, return Unknown, no pruning
-- =============================================================================
-- Part 5: val_int64 [-2^53-4, 2^53+4]
INSERT INTO test_stats_exceeds SELECT '2025-01-05', number, if(number < 5, toInt64(-9007199254740992) - 4 + number, toInt64(9007199254740992) + number - 5), 0, toDecimal128(0, 0) FROM numbers(10);

SELECT '-- Case 5: Int64 min <= -2^53 and max >= 2^53, cannot prune';
WITH has_pr AS (SELECT count() > 0 AS is_pr FROM (EXPLAIN indexes = 1 SELECT count() FROM test_stats_exceeds WHERE val_int64 = 0) WHERE explain LIKE '%ReadFromRemoteParallelReplicas%')
SELECT if((SELECT is_pr FROM has_pr), replaceRegexpOne(explain, '^    ', ''), explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_stats_exceeds WHERE val_int64 = 0) WHERE explain NOT LIKE '%MergingAggregated%' AND explain NOT LIKE '%Union%' AND explain NOT LIKE '%ReadFromRemoteParallelReplicas%';
SELECT count() FROM test_stats_exceeds WHERE val_int64 = 0;

-- =============================================================================
-- Case 6: Decimal128 with values exceeding Float64 precision
-- Statistics: min=2^53, max=2^53+9
-- Expected: Use boundary [2^53, +inf), can prune if query < 2^53
-- =============================================================================
-- Part 6: val_decimal [2^53, 2^53+9]
INSERT INTO test_stats_exceeds SELECT '2025-01-06', number, 0, 0, toDecimal128('9007199254740992', 0) + number FROM numbers(10);

SELECT '-- Case 6: Decimal128 both >= 2^53, query < 2^53, should prune part 6';
WITH has_pr AS (SELECT count() > 0 AS is_pr FROM (EXPLAIN indexes = 1 SELECT count() FROM test_stats_exceeds WHERE val_decimal = toDecimal128(100, 0)) WHERE explain LIKE '%ReadFromRemoteParallelReplicas%')
SELECT if((SELECT is_pr FROM has_pr), replaceRegexpOne(explain, '^    ', ''), explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_stats_exceeds WHERE val_decimal = toDecimal128(100, 0)) WHERE explain NOT LIKE '%MergingAggregated%' AND explain NOT LIKE '%Union%' AND explain NOT LIKE '%ReadFromRemoteParallelReplicas%';
SELECT count() FROM test_stats_exceeds WHERE val_decimal = toDecimal128(100, 0);

SELECT '-- Case 6: Decimal128 both >= 2^53, query >= 2^53, should NOT prune part 6';
WITH has_pr AS (SELECT count() > 0 AS is_pr FROM (EXPLAIN indexes = 1 SELECT count() FROM test_stats_exceeds WHERE val_decimal = toDecimal128('9007199254740995', 0)) WHERE explain LIKE '%ReadFromRemoteParallelReplicas%')
SELECT if((SELECT is_pr FROM has_pr), replaceRegexpOne(explain, '^    ', ''), explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_stats_exceeds WHERE val_decimal = toDecimal128('9007199254740995', 0)) WHERE explain NOT LIKE '%MergingAggregated%' AND explain NOT LIKE '%Union%' AND explain NOT LIKE '%ReadFromRemoteParallelReplicas%';
SELECT count() FROM test_stats_exceeds WHERE val_decimal = toDecimal128('9007199254740995', 0);

-- =============================================================================
-- Case 7: Decimal128 with values exceeding Float64 precision
-- Statistics: min=-2^53-9, max=-2^53
-- Expected: Use boundary (-inf, -2^53], can prune if query > -2^53
-- =============================================================================
-- Part 7: val_decimal [-2^53-9, -2^53] (negative large Decimal)
INSERT INTO test_stats_exceeds SELECT '2025-01-07', number, 0, 0, toDecimal128('-9007199254740992', 0) - 9 + number FROM numbers(10);

SELECT '-- Case 7: Decimal128 both <= -2^53, query > -2^53, should prune part 7';
WITH has_pr AS (SELECT count() > 0 AS is_pr FROM (EXPLAIN indexes = 1 SELECT count() FROM test_stats_exceeds WHERE val_decimal = toDecimal128(-100, 0)) WHERE explain LIKE '%ReadFromRemoteParallelReplicas%')
SELECT if((SELECT is_pr FROM has_pr), replaceRegexpOne(explain, '^    ', ''), explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_stats_exceeds WHERE val_decimal = toDecimal128(-100, 0)) WHERE explain NOT LIKE '%MergingAggregated%' AND explain NOT LIKE '%Union%' AND explain NOT LIKE '%ReadFromRemoteParallelReplicas%';
SELECT count() FROM test_stats_exceeds WHERE val_decimal = toDecimal128(-100, 0);

SELECT '-- Case 7: Decimal128 both <= -2^53, query <= -2^53, should NOT prune part 7';
WITH has_pr AS (SELECT count() > 0 AS is_pr FROM (EXPLAIN indexes = 1 SELECT count() FROM test_stats_exceeds WHERE val_decimal = toDecimal128('-9007199254740995', 0)) WHERE explain LIKE '%ReadFromRemoteParallelReplicas%')
SELECT if((SELECT is_pr FROM has_pr), replaceRegexpOne(explain, '^    ', ''), explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_stats_exceeds WHERE val_decimal = toDecimal128('-9007199254740995', 0)) WHERE explain NOT LIKE '%MergingAggregated%' AND explain NOT LIKE '%Union%' AND explain NOT LIKE '%ReadFromRemoteParallelReplicas%';
SELECT count() FROM test_stats_exceeds WHERE val_decimal = toDecimal128('-9007199254740995', 0);

DROP TABLE test_stats_exceeds;
