-- Tests for correctness of the ordered_group_by_limit_pushdown optimization.
-- Part 1: integer key types (UInt8..UInt256).

-- Tags: no-parallel-replicas, long

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
    val UInt64
) ENGINE = MergeTree ORDER BY k_u64;

INSERT INTO t_gbylimit
SELECT
    (number % 200)::UInt8,
    (number % 10000)::UInt16,
    (number * 7 + 13) % 40000,
    number,
    toUInt128(number),
    toUInt256(number),
    number
FROM numbers(50000);

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

DROP TABLE t_gbylimit;
