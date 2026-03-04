-- Tests for correctness of the ordered_group_by_limit_pushdown optimization.
-- The optimization maintains a bounded heap during aggregation to skip GROUP BY keys
-- that won't appear in the final ORDER BY ... LIMIT top-N results.
-- We test every aggregation method that can be triggered with a single GROUP BY key.

-- Tags: no-parallel-replicas

SET ordered_group_by_limit_pushdown = 1;
SET allow_suspicious_low_cardinality_types = 1;

DROP TABLE IF EXISTS t_gbylimit;

CREATE TABLE t_gbylimit
(
    k_u8 UInt8,
    k_u16 UInt16,
    k_u32 UInt32,
    k_u64 UInt64,
    k_u128 UInt128,
    k_u256 UInt256,
    k_str String,
    k_fstr FixedString(12),
    k_tup Tuple(UInt32, UInt32),
    k_nu32 Nullable(UInt32),
    k_nstr Nullable(String),
    k_lcu64 LowCardinality(UInt64),
    k_lcstr LowCardinality(String),
    val UInt64
) ENGINE = MergeTree ORDER BY k_u64;

-- Insert deterministic data. Use number as seed for reproducibility.
-- ~50000 rows, keys have various cardinalities depending on type.
INSERT INTO t_gbylimit
SELECT
    (number % 200)::UInt8,
    (number % 10000)::UInt16,
    (number * 7 + 13) % 40000,
    number,
    toUInt128(number),
    toUInt256(number),
    toString(number % 30000),
    toFixedString(toString(number % 25000), 12),
    tuple((number % 20000)::UInt32, ((number * 3) % 20000)::UInt32),
    if(number % 97 = 0, NULL, (number % 35000)::UInt32),
    if(number % 83 = 0, NULL, toString(number % 30000)),
    number % 45000,
    toString(number % 28000),
    number
FROM numbers(50000);

-- Helper: for each aggregation method, compare results WITH vs WITHOUT the optimization.
-- If the optimization is correct, both must return identical rows.

-- =====================
-- key8 (UInt8, 256 possible values)
-- =====================
SELECT 'key8';
SELECT k_u8, count(), sum(val)
FROM t_gbylimit GROUP BY k_u8 ORDER BY k_u8 ASC LIMIT 10
SETTINGS ordered_group_by_limit_pushdown = 1
EXCEPT
SELECT k_u8, count(), sum(val)
FROM t_gbylimit GROUP BY k_u8 ORDER BY k_u8 ASC LIMIT 10
SETTINGS ordered_group_by_limit_pushdown = 0;

-- =====================
-- key16 (UInt16)
-- =====================
SELECT 'key16';
SELECT k_u16, count(), sum(val)
FROM t_gbylimit GROUP BY k_u16 ORDER BY k_u16 ASC LIMIT 10
SETTINGS ordered_group_by_limit_pushdown = 1
EXCEPT
SELECT k_u16, count(), sum(val)
FROM t_gbylimit GROUP BY k_u16 ORDER BY k_u16 ASC LIMIT 10
SETTINGS ordered_group_by_limit_pushdown = 0;

-- =====================
-- key32 (UInt32)
-- =====================
SELECT 'key32';
SELECT k_u32, count(), sum(val)
FROM t_gbylimit GROUP BY k_u32 ORDER BY k_u32 ASC LIMIT 10
SETTINGS ordered_group_by_limit_pushdown = 1
EXCEPT
SELECT k_u32, count(), sum(val)
FROM t_gbylimit GROUP BY k_u32 ORDER BY k_u32 ASC LIMIT 10
SETTINGS ordered_group_by_limit_pushdown = 0;

-- =====================
-- key64 (UInt64)
-- =====================
SELECT 'key64';
SELECT k_u64, count(), sum(val)
FROM t_gbylimit GROUP BY k_u64 ORDER BY k_u64 ASC LIMIT 10
SETTINGS ordered_group_by_limit_pushdown = 1
EXCEPT
SELECT k_u64, count(), sum(val)
FROM t_gbylimit GROUP BY k_u64 ORDER BY k_u64 ASC LIMIT 10
SETTINGS ordered_group_by_limit_pushdown = 0;

-- =====================
-- keys128 (UInt128)
-- =====================
SELECT 'keys128';
SELECT k_u128, count(), sum(val)
FROM t_gbylimit GROUP BY k_u128 ORDER BY k_u128 ASC LIMIT 10
SETTINGS ordered_group_by_limit_pushdown = 1
EXCEPT
SELECT k_u128, count(), sum(val)
FROM t_gbylimit GROUP BY k_u128 ORDER BY k_u128 ASC LIMIT 10
SETTINGS ordered_group_by_limit_pushdown = 0;

-- =====================
-- keys256 (UInt256)
-- =====================
SELECT 'keys256';
SELECT k_u256, count(), sum(val)
FROM t_gbylimit GROUP BY k_u256 ORDER BY k_u256 ASC LIMIT 10
SETTINGS ordered_group_by_limit_pushdown = 1
EXCEPT
SELECT k_u256, count(), sum(val)
FROM t_gbylimit GROUP BY k_u256 ORDER BY k_u256 ASC LIMIT 10
SETTINGS ordered_group_by_limit_pushdown = 0;

-- =====================
-- key_string (String)
-- =====================
SELECT 'key_string';
SELECT k_str, count(), sum(val)
FROM t_gbylimit GROUP BY k_str ORDER BY k_str ASC LIMIT 10
SETTINGS ordered_group_by_limit_pushdown = 1
EXCEPT
SELECT k_str, count(), sum(val)
FROM t_gbylimit GROUP BY k_str ORDER BY k_str ASC LIMIT 10
SETTINGS ordered_group_by_limit_pushdown = 0;

-- =====================
-- key_fixed_string (FixedString)
-- =====================
SELECT 'key_fixed_string';
SELECT k_fstr, count(), sum(val)
FROM t_gbylimit GROUP BY k_fstr ORDER BY k_fstr ASC LIMIT 10
SETTINGS ordered_group_by_limit_pushdown = 1
EXCEPT
SELECT k_fstr, count(), sum(val)
FROM t_gbylimit GROUP BY k_fstr ORDER BY k_fstr ASC LIMIT 10
SETTINGS ordered_group_by_limit_pushdown = 0;

-- =====================
-- serialized (Tuple)
-- =====================
SELECT 'serialized';
SELECT k_tup, count(), sum(val)
FROM t_gbylimit GROUP BY k_tup ORDER BY k_tup ASC LIMIT 10
SETTINGS ordered_group_by_limit_pushdown = 1
EXCEPT
SELECT k_tup, count(), sum(val)
FROM t_gbylimit GROUP BY k_tup ORDER BY k_tup ASC LIMIT 10
SETTINGS ordered_group_by_limit_pushdown = 0;

-- =====================
-- nullable_key32 (Nullable(UInt32))
-- =====================
SELECT 'nullable_key32';
SELECT k_nu32, count(), sum(val)
FROM t_gbylimit GROUP BY k_nu32 ORDER BY k_nu32 ASC LIMIT 10
SETTINGS ordered_group_by_limit_pushdown = 1
EXCEPT
SELECT k_nu32, count(), sum(val)
FROM t_gbylimit GROUP BY k_nu32 ORDER BY k_nu32 ASC LIMIT 10
SETTINGS ordered_group_by_limit_pushdown = 0;

-- =====================
-- nullable_key_string (Nullable(String))
-- =====================
SELECT 'nullable_key_string';
SELECT k_nstr, count(), sum(val)
FROM t_gbylimit GROUP BY k_nstr ORDER BY k_nstr ASC LIMIT 10
SETTINGS ordered_group_by_limit_pushdown = 1
EXCEPT
SELECT k_nstr, count(), sum(val)
FROM t_gbylimit GROUP BY k_nstr ORDER BY k_nstr ASC LIMIT 10
SETTINGS ordered_group_by_limit_pushdown = 0;

-- =====================
-- low_cardinality_key64 (LowCardinality(UInt64))
-- =====================
SELECT 'low_cardinality_key64';
SELECT k_lcu64, count(), sum(val)
FROM t_gbylimit GROUP BY k_lcu64 ORDER BY k_lcu64 ASC LIMIT 10
SETTINGS ordered_group_by_limit_pushdown = 1
EXCEPT
SELECT k_lcu64, count(), sum(val)
FROM t_gbylimit GROUP BY k_lcu64 ORDER BY k_lcu64 ASC LIMIT 10
SETTINGS ordered_group_by_limit_pushdown = 0;

-- =====================
-- low_cardinality_key_string (LowCardinality(String))
-- =====================
SELECT 'low_cardinality_key_string';
SELECT k_lcstr, count(), sum(val)
FROM t_gbylimit GROUP BY k_lcstr ORDER BY k_lcstr ASC LIMIT 10
SETTINGS ordered_group_by_limit_pushdown = 1
EXCEPT
SELECT k_lcstr, count(), sum(val)
FROM t_gbylimit GROUP BY k_lcstr ORDER BY k_lcstr ASC LIMIT 10
SETTINGS ordered_group_by_limit_pushdown = 0;

-- =====================
-- Test with LIMIT offset (LIMIT 5, 10)
-- =====================
SELECT 'limit_with_offset';
SELECT k_u64, count(), sum(val)
FROM t_gbylimit GROUP BY k_u64 ORDER BY k_u64 ASC LIMIT 5, 10
SETTINGS ordered_group_by_limit_pushdown = 1
EXCEPT
SELECT k_u64, count(), sum(val)
FROM t_gbylimit GROUP BY k_u64 ORDER BY k_u64 ASC LIMIT 5, 10
SETTINGS ordered_group_by_limit_pushdown = 0;

-- =====================
-- Test with multiple aggregate functions
-- =====================
SELECT 'multiple_aggregates';
SELECT k_u32, count(), sum(val), min(val), max(val), avg(val)
FROM t_gbylimit GROUP BY k_u32 ORDER BY k_u32 ASC LIMIT 10
SETTINGS ordered_group_by_limit_pushdown = 1
EXCEPT
SELECT k_u32, count(), sum(val), min(val), max(val), avg(val)
FROM t_gbylimit GROUP BY k_u32 ORDER BY k_u32 ASC LIMIT 10
SETTINGS ordered_group_by_limit_pushdown = 0;

-- =====================
-- Negative tests: optimization must NOT be applied in these cases.
-- We verify that results still match (the optimization gracefully disables).
-- =====================

-- DESC order (not supported yet)
SELECT 'negative_desc';
SELECT k_u64, count()
FROM t_gbylimit GROUP BY k_u64 ORDER BY k_u64 DESC LIMIT 10
SETTINGS ordered_group_by_limit_pushdown = 1
EXCEPT
SELECT k_u64, count()
FROM t_gbylimit GROUP BY k_u64 ORDER BY k_u64 DESC LIMIT 10
SETTINGS ordered_group_by_limit_pushdown = 0;

-- WITH TOTALS (overflow_row): the optimization should NOT be applied.
-- Totals rows propagate through subqueries, so we use a materializing
-- CTE to strip them before comparing.
SELECT 'negative_with_totals';
WITH
    a AS (SELECT k_u32, count() AS cnt FROM t_gbylimit GROUP BY k_u32 WITH TOTALS ORDER BY k_u32 ASC LIMIT 10 SETTINGS ordered_group_by_limit_pushdown = 1),
    b AS (SELECT k_u32, count() AS cnt FROM t_gbylimit GROUP BY k_u32 WITH TOTALS ORDER BY k_u32 ASC LIMIT 10 SETTINGS ordered_group_by_limit_pushdown = 0)
SELECT count() FROM (SELECT * FROM a EXCEPT SELECT * FROM b);

-- HAVING (FilterStep breaks pattern)
SELECT 'negative_having';
SELECT k_u32, count() AS cnt
FROM t_gbylimit GROUP BY k_u32 HAVING cnt > 1 ORDER BY k_u32 ASC LIMIT 10
SETTINGS ordered_group_by_limit_pushdown = 1
EXCEPT
SELECT k_u32, count() AS cnt
FROM t_gbylimit GROUP BY k_u32 HAVING cnt > 1 ORDER BY k_u32 ASC LIMIT 10
SETTINGS ordered_group_by_limit_pushdown = 0;

-- ORDER BY aggregate (not GROUP BY key): add tie-breaker for determinism
SELECT 'negative_order_by_aggregate';
SELECT k_u32, count() AS cnt
FROM t_gbylimit GROUP BY k_u32 ORDER BY cnt DESC, k_u32 ASC LIMIT 10
SETTINGS ordered_group_by_limit_pushdown = 1
EXCEPT
SELECT k_u32, count() AS cnt
FROM t_gbylimit GROUP BY k_u32 ORDER BY cnt DESC, k_u32 ASC LIMIT 10
SETTINGS ordered_group_by_limit_pushdown = 0;

-- =====================
-- Test two-level hash table conversion (happens with high cardinality).
-- The bounded heap must survive the single-to-two-level migration.
-- =====================
SELECT 'two_level';
SELECT number, count()
FROM numbers(2000000) GROUP BY number ORDER BY number ASC LIMIT 10
SETTINGS ordered_group_by_limit_pushdown = 1
EXCEPT
SELECT number, count()
FROM numbers(2000000) GROUP BY number ORDER BY number ASC LIMIT 10
SETTINGS ordered_group_by_limit_pushdown = 0;

-- =====================
-- Test two-level with String key
-- =====================
SELECT 'two_level_string';
SELECT toString(number) AS k, count()
FROM numbers(2000000) GROUP BY k ORDER BY k ASC LIMIT 10
SETTINGS ordered_group_by_limit_pushdown = 1
EXCEPT
SELECT toString(number) AS k, count()
FROM numbers(2000000) GROUP BY k ORDER BY k ASC LIMIT 10
SETTINGS ordered_group_by_limit_pushdown = 0;

-- Multiple GROUP BY keys (not yet supported)
SELECT 'negative_multi_key';
SELECT k_u32, k_u64, count()
FROM t_gbylimit GROUP BY k_u32, k_u64 ORDER BY k_u32, k_u64 ASC LIMIT 10
SETTINGS ordered_group_by_limit_pushdown = 1
EXCEPT
SELECT k_u32, k_u64, count()
FROM t_gbylimit GROUP BY k_u32, k_u64 ORDER BY k_u32, k_u64 ASC LIMIT 10
SETTINGS ordered_group_by_limit_pushdown = 0;

DROP TABLE t_gbylimit;
