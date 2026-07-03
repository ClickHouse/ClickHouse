-- Tags: no-parallel-replicas, long
-- Correctness of enable_group_by_top_k_optimization across GROUP BY key types:
-- unsigned and signed integers, Date32/DateTime, Float32/Float64 (NaN handling)
-- on the typed numeric fast path, strings, tuples, nullable and low-cardinality
-- keys.  Every case compares the optimized result against the same query with
-- the optimization off; an empty EXCEPT result means they match.

-- CI profile sets max_rows_to_group_by, which disables the optimization; reset it.
SET max_rows_to_group_by = 0;
-- CI randomizes query_plan_max_limit_for_top_k_optimization (can be tiny); pin it.
SET query_plan_max_limit_for_top_k_optimization = 1000;

SET enable_group_by_top_k_optimization = 1;
SET allow_suspicious_low_cardinality_types = 1;

-- Unsigned integer keys (UInt8..UInt256).

DROP TABLE IF EXISTS t_gbylimit_uint;

CREATE TABLE t_gbylimit_uint
(
    k_u8 UInt8,
    k_u16 UInt16,
    k_u32 UInt32,
    k_u64 UInt64,
    k_u128 UInt128,
    k_u256 UInt256,
    val UInt64
) ENGINE = MergeTree ORDER BY k_u64;

INSERT INTO t_gbylimit_uint
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
FROM t_gbylimit_uint GROUP BY k_u8 ORDER BY k_u8 ASC LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 1
EXCEPT
SELECT k_u8, count(), sum(val)
FROM t_gbylimit_uint GROUP BY k_u8 ORDER BY k_u8 ASC LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 0;

SELECT 'key16';
SELECT k_u16, count(), sum(val)
FROM t_gbylimit_uint GROUP BY k_u16 ORDER BY k_u16 ASC LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 1
EXCEPT
SELECT k_u16, count(), sum(val)
FROM t_gbylimit_uint GROUP BY k_u16 ORDER BY k_u16 ASC LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 0;

SELECT 'key32';
SELECT k_u32, count(), sum(val)
FROM t_gbylimit_uint GROUP BY k_u32 ORDER BY k_u32 ASC LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 1
EXCEPT
SELECT k_u32, count(), sum(val)
FROM t_gbylimit_uint GROUP BY k_u32 ORDER BY k_u32 ASC LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 0;

SELECT 'key64';
SELECT k_u64, count(), sum(val)
FROM t_gbylimit_uint GROUP BY k_u64 ORDER BY k_u64 ASC LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 1
EXCEPT
SELECT k_u64, count(), sum(val)
FROM t_gbylimit_uint GROUP BY k_u64 ORDER BY k_u64 ASC LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 0;

SELECT 'keys128';
SELECT k_u128, count(), sum(val)
FROM t_gbylimit_uint GROUP BY k_u128 ORDER BY k_u128 ASC LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 1
EXCEPT
SELECT k_u128, count(), sum(val)
FROM t_gbylimit_uint GROUP BY k_u128 ORDER BY k_u128 ASC LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 0;

SELECT 'keys256';
SELECT k_u256, count(), sum(val)
FROM t_gbylimit_uint GROUP BY k_u256 ORDER BY k_u256 ASC LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 1
EXCEPT
SELECT k_u256, count(), sum(val)
FROM t_gbylimit_uint GROUP BY k_u256 ORDER BY k_u256 ASC LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 0;

DROP TABLE t_gbylimit_uint;

-- Signed integer and Date32 keys, both directions.

DROP TABLE IF EXISTS t_gbylimit_signed;

CREATE TABLE t_gbylimit_signed
(
    k_i8 Int8,
    k_i16 Int16,
    k_i32 Int32,
    k_i64 Int64,
    k_d32 Date32,
    val UInt64
) ENGINE = MergeTree ORDER BY k_i64;

INSERT INTO t_gbylimit_signed
SELECT
    ((number % 200) - 100)::Int8,
    ((number % 10000) - 5000)::Int16,
    ((number * 7 + 13) % 40000 - 20000)::Int32,
    (number::Int64 - 25000),
    toDate32('2020-01-01') + INTERVAL ((number % 10000) - 5000) DAY,
    number
FROM numbers(50000);

SELECT 'int8_asc';
SELECT k_i8, count(), sum(val)
FROM t_gbylimit_signed GROUP BY k_i8 ORDER BY k_i8 ASC LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 1
EXCEPT
SELECT k_i8, count(), sum(val)
FROM t_gbylimit_signed GROUP BY k_i8 ORDER BY k_i8 ASC LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 0;

SELECT 'int8_desc';
SELECT k_i8, count(), sum(val)
FROM t_gbylimit_signed GROUP BY k_i8 ORDER BY k_i8 DESC LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 1
EXCEPT
SELECT k_i8, count(), sum(val)
FROM t_gbylimit_signed GROUP BY k_i8 ORDER BY k_i8 DESC LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 0;

SELECT 'int16_asc';
SELECT k_i16, count(), sum(val)
FROM t_gbylimit_signed GROUP BY k_i16 ORDER BY k_i16 ASC LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 1
EXCEPT
SELECT k_i16, count(), sum(val)
FROM t_gbylimit_signed GROUP BY k_i16 ORDER BY k_i16 ASC LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 0;

SELECT 'int16_desc';
SELECT k_i16, count(), sum(val)
FROM t_gbylimit_signed GROUP BY k_i16 ORDER BY k_i16 DESC LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 1
EXCEPT
SELECT k_i16, count(), sum(val)
FROM t_gbylimit_signed GROUP BY k_i16 ORDER BY k_i16 DESC LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 0;

SELECT 'int32_asc';
SELECT k_i32, count(), sum(val)
FROM t_gbylimit_signed GROUP BY k_i32 ORDER BY k_i32 ASC LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 1
EXCEPT
SELECT k_i32, count(), sum(val)
FROM t_gbylimit_signed GROUP BY k_i32 ORDER BY k_i32 ASC LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 0;

SELECT 'int32_desc';
SELECT k_i32, count(), sum(val)
FROM t_gbylimit_signed GROUP BY k_i32 ORDER BY k_i32 DESC LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 1
EXCEPT
SELECT k_i32, count(), sum(val)
FROM t_gbylimit_signed GROUP BY k_i32 ORDER BY k_i32 DESC LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 0;

SELECT 'int64_asc';
SELECT k_i64, count(), sum(val)
FROM t_gbylimit_signed GROUP BY k_i64 ORDER BY k_i64 ASC LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 1
EXCEPT
SELECT k_i64, count(), sum(val)
FROM t_gbylimit_signed GROUP BY k_i64 ORDER BY k_i64 ASC LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 0;

SELECT 'int64_desc';
SELECT k_i64, count(), sum(val)
FROM t_gbylimit_signed GROUP BY k_i64 ORDER BY k_i64 DESC LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 1
EXCEPT
SELECT k_i64, count(), sum(val)
FROM t_gbylimit_signed GROUP BY k_i64 ORDER BY k_i64 DESC LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 0;

SELECT 'date32_asc';
SELECT k_d32, count(), sum(val)
FROM t_gbylimit_signed GROUP BY k_d32 ORDER BY k_d32 ASC LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 1
EXCEPT
SELECT k_d32, count(), sum(val)
FROM t_gbylimit_signed GROUP BY k_d32 ORDER BY k_d32 ASC LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 0;

SELECT 'date32_desc';
SELECT k_d32, count(), sum(val)
FROM t_gbylimit_signed GROUP BY k_d32 ORDER BY k_d32 DESC LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 1
EXCEPT
SELECT k_d32, count(), sum(val)
FROM t_gbylimit_signed GROUP BY k_d32 ORDER BY k_d32 DESC LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 0;

DROP TABLE t_gbylimit_signed;

-- Float32/Float64 keys with NaNs, all direction/NULLS combinations.

DROP TABLE IF EXISTS t_gbylimit_float;

CREATE TABLE t_gbylimit_float
(
    k_f32 Float32,
    k_f64 Float64,
    val UInt64
) ENGINE = MergeTree ORDER BY val;

INSERT INTO t_gbylimit_float
SELECT
    if(number % 500 = 0, nan, ((number * 7 + 13) % 40000 - 20000)::Float32 / 7.0),
    if(number % 500 = 1, nan, ((number * 11 + 3) % 60000 - 30000)::Float64 / 11.0),
    number
FROM numbers(50000);

SELECT 'float32_asc';
SELECT k_f32, count(), sum(val)
FROM t_gbylimit_float GROUP BY k_f32 ORDER BY k_f32 ASC LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 1
EXCEPT
SELECT k_f32, count(), sum(val)
FROM t_gbylimit_float GROUP BY k_f32 ORDER BY k_f32 ASC LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 0;

SELECT 'float32_desc';
SELECT k_f32, count(), sum(val)
FROM t_gbylimit_float GROUP BY k_f32 ORDER BY k_f32 DESC LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 1
EXCEPT
SELECT k_f32, count(), sum(val)
FROM t_gbylimit_float GROUP BY k_f32 ORDER BY k_f32 DESC LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 0;

SELECT 'float64_asc';
SELECT k_f64, count(), sum(val)
FROM t_gbylimit_float GROUP BY k_f64 ORDER BY k_f64 ASC LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 1
EXCEPT
SELECT k_f64, count(), sum(val)
FROM t_gbylimit_float GROUP BY k_f64 ORDER BY k_f64 ASC LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 0;

SELECT 'float64_desc';
SELECT k_f64, count(), sum(val)
FROM t_gbylimit_float GROUP BY k_f64 ORDER BY k_f64 DESC LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 1
EXCEPT
SELECT k_f64, count(), sum(val)
FROM t_gbylimit_float GROUP BY k_f64 ORDER BY k_f64 DESC LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 0;

SELECT 'float32_asc_nulls_first';
SELECT k_f32, count(), sum(val)
FROM t_gbylimit_float GROUP BY k_f32 ORDER BY k_f32 ASC NULLS FIRST LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 1
EXCEPT
SELECT k_f32, count(), sum(val)
FROM t_gbylimit_float GROUP BY k_f32 ORDER BY k_f32 ASC NULLS FIRST LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 0;

SELECT 'float32_asc_nulls_last';
SELECT k_f32, count(), sum(val)
FROM t_gbylimit_float GROUP BY k_f32 ORDER BY k_f32 ASC NULLS LAST LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 1
EXCEPT
SELECT k_f32, count(), sum(val)
FROM t_gbylimit_float GROUP BY k_f32 ORDER BY k_f32 ASC NULLS LAST LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 0;

SELECT 'float32_desc_nulls_first';
SELECT k_f32, count(), sum(val)
FROM t_gbylimit_float GROUP BY k_f32 ORDER BY k_f32 DESC NULLS FIRST LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 1
EXCEPT
SELECT k_f32, count(), sum(val)
FROM t_gbylimit_float GROUP BY k_f32 ORDER BY k_f32 DESC NULLS FIRST LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 0;

SELECT 'float32_desc_nulls_last';
SELECT k_f32, count(), sum(val)
FROM t_gbylimit_float GROUP BY k_f32 ORDER BY k_f32 DESC NULLS LAST LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 1
EXCEPT
SELECT k_f32, count(), sum(val)
FROM t_gbylimit_float GROUP BY k_f32 ORDER BY k_f32 DESC NULLS LAST LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 0;

SELECT 'float64_asc_nulls_first';
SELECT k_f64, count(), sum(val)
FROM t_gbylimit_float GROUP BY k_f64 ORDER BY k_f64 ASC NULLS FIRST LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 1
EXCEPT
SELECT k_f64, count(), sum(val)
FROM t_gbylimit_float GROUP BY k_f64 ORDER BY k_f64 ASC NULLS FIRST LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 0;

SELECT 'float64_desc_nulls_last';
SELECT k_f64, count(), sum(val)
FROM t_gbylimit_float GROUP BY k_f64 ORDER BY k_f64 DESC NULLS LAST LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 1
EXCEPT
SELECT k_f64, count(), sum(val)
FROM t_gbylimit_float GROUP BY k_f64 ORDER BY k_f64 DESC NULLS LAST LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 0;

DROP TABLE t_gbylimit_float;

-- DateTime keys on the typed numeric fast path.

DROP TABLE IF EXISTS t_gbylimit_datetime;

CREATE TABLE t_gbylimit_datetime
(
    k_dt DateTime,
    val UInt64
) ENGINE = MergeTree ORDER BY val;

INSERT INTO t_gbylimit_datetime
SELECT
    toDateTime('2010-01-01 00:00:00') + INTERVAL (number * 97 + 17) SECOND,
    number
FROM numbers(50000);

SELECT 'datetime_asc';
SELECT k_dt, count(), sum(val)
FROM t_gbylimit_datetime GROUP BY k_dt ORDER BY k_dt ASC LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 1
EXCEPT
SELECT k_dt, count(), sum(val)
FROM t_gbylimit_datetime GROUP BY k_dt ORDER BY k_dt ASC LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 0;

SELECT 'datetime_desc';
SELECT k_dt, count(), sum(val)
FROM t_gbylimit_datetime GROUP BY k_dt ORDER BY k_dt DESC LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 1
EXCEPT
SELECT k_dt, count(), sum(val)
FROM t_gbylimit_datetime GROUP BY k_dt ORDER BY k_dt DESC LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 0;

DROP TABLE t_gbylimit_datetime;

-- String, FixedString, serialized tuple, nullable, and low-cardinality keys.

DROP TABLE IF EXISTS t_gbylimit_str;

CREATE TABLE t_gbylimit_str
(
    k_str String,
    k_fstr FixedString(12),
    k_tup Tuple(UInt32, UInt32),
    k_nu32 Nullable(UInt32),
    k_nstr Nullable(String),
    k_lcu64 LowCardinality(UInt64),
    k_lcstr LowCardinality(String),
    val UInt64
) ENGINE = MergeTree ORDER BY k_str;

INSERT INTO t_gbylimit_str
SELECT
    toString(number % 30000),
    toFixedString(toString(number % 25000), 12),
    tuple((number % 20000)::UInt32, ((number * 3) % 20000)::UInt32),
    if(number % 97 = 0, NULL, (number % 35000)::UInt32),
    if(number % 83 = 0, NULL, toString(number % 30000)),
    number % 45000,
    toString(number % 28000),
    number
FROM numbers(50000);

SELECT 'key_string';
SELECT k_str, count(), sum(val)
FROM t_gbylimit_str GROUP BY k_str ORDER BY k_str ASC LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 1
EXCEPT
SELECT k_str, count(), sum(val)
FROM t_gbylimit_str GROUP BY k_str ORDER BY k_str ASC LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 0;

SELECT 'key_fixed_string';
SELECT k_fstr, count(), sum(val)
FROM t_gbylimit_str GROUP BY k_fstr ORDER BY k_fstr ASC LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 1
EXCEPT
SELECT k_fstr, count(), sum(val)
FROM t_gbylimit_str GROUP BY k_fstr ORDER BY k_fstr ASC LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 0;

SELECT 'serialized';
SELECT k_tup, count(), sum(val)
FROM t_gbylimit_str GROUP BY k_tup ORDER BY k_tup ASC LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 1
EXCEPT
SELECT k_tup, count(), sum(val)
FROM t_gbylimit_str GROUP BY k_tup ORDER BY k_tup ASC LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 0;

SELECT 'nullable_key32';
SELECT k_nu32, count(), sum(val)
FROM t_gbylimit_str GROUP BY k_nu32 ORDER BY k_nu32 ASC LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 1
EXCEPT
SELECT k_nu32, count(), sum(val)
FROM t_gbylimit_str GROUP BY k_nu32 ORDER BY k_nu32 ASC LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 0;

SELECT 'nullable_key_string';
SELECT k_nstr, count(), sum(val)
FROM t_gbylimit_str GROUP BY k_nstr ORDER BY k_nstr ASC LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 1
EXCEPT
SELECT k_nstr, count(), sum(val)
FROM t_gbylimit_str GROUP BY k_nstr ORDER BY k_nstr ASC LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 0;

SELECT 'low_cardinality_key64';
SELECT k_lcu64, count(), sum(val)
FROM t_gbylimit_str GROUP BY k_lcu64 ORDER BY k_lcu64 ASC LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 1
EXCEPT
SELECT k_lcu64, count(), sum(val)
FROM t_gbylimit_str GROUP BY k_lcu64 ORDER BY k_lcu64 ASC LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 0;

SELECT 'low_cardinality_key_string';
SELECT k_lcstr, count(), sum(val)
FROM t_gbylimit_str GROUP BY k_lcstr ORDER BY k_lcstr ASC LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 1
EXCEPT
SELECT k_lcstr, count(), sum(val)
FROM t_gbylimit_str GROUP BY k_lcstr ORDER BY k_lcstr ASC LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 0;

DROP TABLE t_gbylimit_str;

SELECT 'optimization_applied_guard';
SELECT count() FROM (EXPLAIN actions = 1 SELECT number AS k FROM numbers(100) GROUP BY k ORDER BY k LIMIT 5) WHERE explain LIKE '%Top-K%';
