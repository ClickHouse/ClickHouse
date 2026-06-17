-- Correctness of enable_group_by_top_k_optimization for integer key types (UInt8..UInt256).

-- Tags: no-parallel-replicas, long

-- CI profile sets max_rows_to_group_by, which disables the optimization; reset it.
SET max_rows_to_group_by = 0;
-- CI randomizes query_plan_max_limit_for_top_k_optimization (can be tiny); pin it.
SET query_plan_max_limit_for_top_k_optimization = 1000;

SET enable_group_by_top_k_optimization = 1;
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

SELECT 'key8';
SELECT k_u8, count(), sum(val)
FROM t_gbylimit GROUP BY k_u8 ORDER BY k_u8 ASC LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 1
EXCEPT
SELECT k_u8, count(), sum(val)
FROM t_gbylimit GROUP BY k_u8 ORDER BY k_u8 ASC LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 0;

SELECT 'key16';
SELECT k_u16, count(), sum(val)
FROM t_gbylimit GROUP BY k_u16 ORDER BY k_u16 ASC LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 1
EXCEPT
SELECT k_u16, count(), sum(val)
FROM t_gbylimit GROUP BY k_u16 ORDER BY k_u16 ASC LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 0;

SELECT 'key32';
SELECT k_u32, count(), sum(val)
FROM t_gbylimit GROUP BY k_u32 ORDER BY k_u32 ASC LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 1
EXCEPT
SELECT k_u32, count(), sum(val)
FROM t_gbylimit GROUP BY k_u32 ORDER BY k_u32 ASC LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 0;

SELECT 'key64';
SELECT k_u64, count(), sum(val)
FROM t_gbylimit GROUP BY k_u64 ORDER BY k_u64 ASC LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 1
EXCEPT
SELECT k_u64, count(), sum(val)
FROM t_gbylimit GROUP BY k_u64 ORDER BY k_u64 ASC LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 0;

SELECT 'keys128';
SELECT k_u128, count(), sum(val)
FROM t_gbylimit GROUP BY k_u128 ORDER BY k_u128 ASC LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 1
EXCEPT
SELECT k_u128, count(), sum(val)
FROM t_gbylimit GROUP BY k_u128 ORDER BY k_u128 ASC LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 0;

SELECT 'keys256';
SELECT k_u256, count(), sum(val)
FROM t_gbylimit GROUP BY k_u256 ORDER BY k_u256 ASC LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 1
EXCEPT
SELECT k_u256, count(), sum(val)
FROM t_gbylimit GROUP BY k_u256 ORDER BY k_u256 ASC LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 0;

DROP TABLE t_gbylimit;

-- Guard against the environment silently disabling the optimization.
SELECT 'optimization_applied_guard';
SELECT count() FROM (EXPLAIN actions = 1 SELECT number AS k FROM numbers(100) GROUP BY k ORDER BY k LIMIT 5) WHERE explain LIKE '%Top-K%';
