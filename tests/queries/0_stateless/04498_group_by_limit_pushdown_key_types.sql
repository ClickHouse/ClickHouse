-- Tags: no-parallel-replicas, long
-- Correctness of enable_group_by_top_k_optimization across GROUP BY key types:
-- unsigned and signed integers, Date32/DateTime, Float32/Float64 (NaN handling)
-- on the typed numeric fast path, strings, tuples, nullable and low-cardinality
-- keys.  Every case compares the optimized result against the same query with
-- the optimization off; an empty EXCEPT result means they match.

-- The top-K optimization does not apply to serialized plans; pin the setting
-- so the assertions hold in the distributed-plan suite.
SET serialize_query_plan = 0;

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

-- `enable_group_by_top_k_optimization` takes effect per query, not per
-- subquery: inside a single statement the last `SETTINGS` clause wins for the
-- whole query, so an on-versus-off comparison written as one `EXCEPT` runs both
-- sides in the same mode and proves nothing.  Each comparison below therefore
-- materializes the unoptimized answer with its own statement first.
DROP TABLE IF EXISTS gt_key8;
CREATE TABLE gt_key8 ENGINE = Memory EMPTY AS
SELECT k_u8, count(), sum(val)
FROM t_gbylimit_uint GROUP BY k_u8 ORDER BY k_u8 ASC LIMIT 10;
SET enable_group_by_top_k_optimization = 0;
INSERT INTO gt_key8
SELECT k_u8, count(), sum(val)
FROM t_gbylimit_uint GROUP BY k_u8 ORDER BY k_u8 ASC LIMIT 10;
SET enable_group_by_top_k_optimization = 1;

SELECT 'key8';
SELECT k_u8, count(), sum(val)
FROM t_gbylimit_uint GROUP BY k_u8 ORDER BY k_u8 ASC LIMIT 10
EXCEPT
SELECT * FROM gt_key8;
SELECT * FROM (SELECT * FROM gt_key8)
EXCEPT
SELECT * FROM (SELECT k_u8, count(), sum(val)
FROM t_gbylimit_uint GROUP BY k_u8 ORDER BY k_u8 ASC LIMIT 10);

DROP TABLE IF EXISTS gt_key16;
CREATE TABLE gt_key16 ENGINE = Memory EMPTY AS
SELECT k_u16, count(), sum(val)
FROM t_gbylimit_uint GROUP BY k_u16 ORDER BY k_u16 ASC LIMIT 10;
SET enable_group_by_top_k_optimization = 0;
INSERT INTO gt_key16
SELECT k_u16, count(), sum(val)
FROM t_gbylimit_uint GROUP BY k_u16 ORDER BY k_u16 ASC LIMIT 10;
SET enable_group_by_top_k_optimization = 1;

SELECT 'key16';
SELECT k_u16, count(), sum(val)
FROM t_gbylimit_uint GROUP BY k_u16 ORDER BY k_u16 ASC LIMIT 10
EXCEPT
SELECT * FROM gt_key16;
SELECT * FROM (SELECT * FROM gt_key16)
EXCEPT
SELECT * FROM (SELECT k_u16, count(), sum(val)
FROM t_gbylimit_uint GROUP BY k_u16 ORDER BY k_u16 ASC LIMIT 10);

DROP TABLE IF EXISTS gt_key32;
CREATE TABLE gt_key32 ENGINE = Memory EMPTY AS
SELECT k_u32, count(), sum(val)
FROM t_gbylimit_uint GROUP BY k_u32 ORDER BY k_u32 ASC LIMIT 10;
SET enable_group_by_top_k_optimization = 0;
INSERT INTO gt_key32
SELECT k_u32, count(), sum(val)
FROM t_gbylimit_uint GROUP BY k_u32 ORDER BY k_u32 ASC LIMIT 10;
SET enable_group_by_top_k_optimization = 1;

SELECT 'key32';
SELECT k_u32, count(), sum(val)
FROM t_gbylimit_uint GROUP BY k_u32 ORDER BY k_u32 ASC LIMIT 10
EXCEPT
SELECT * FROM gt_key32;
SELECT * FROM (SELECT * FROM gt_key32)
EXCEPT
SELECT * FROM (SELECT k_u32, count(), sum(val)
FROM t_gbylimit_uint GROUP BY k_u32 ORDER BY k_u32 ASC LIMIT 10);

DROP TABLE IF EXISTS gt_key64;
CREATE TABLE gt_key64 ENGINE = Memory EMPTY AS
SELECT k_u64, count(), sum(val)
FROM t_gbylimit_uint GROUP BY k_u64 ORDER BY k_u64 ASC LIMIT 10;
SET enable_group_by_top_k_optimization = 0;
INSERT INTO gt_key64
SELECT k_u64, count(), sum(val)
FROM t_gbylimit_uint GROUP BY k_u64 ORDER BY k_u64 ASC LIMIT 10;
SET enable_group_by_top_k_optimization = 1;

SELECT 'key64';
SELECT k_u64, count(), sum(val)
FROM t_gbylimit_uint GROUP BY k_u64 ORDER BY k_u64 ASC LIMIT 10
EXCEPT
SELECT * FROM gt_key64;
SELECT * FROM (SELECT * FROM gt_key64)
EXCEPT
SELECT * FROM (SELECT k_u64, count(), sum(val)
FROM t_gbylimit_uint GROUP BY k_u64 ORDER BY k_u64 ASC LIMIT 10);

DROP TABLE IF EXISTS gt_keys128;
CREATE TABLE gt_keys128 ENGINE = Memory EMPTY AS
SELECT k_u128, count(), sum(val)
FROM t_gbylimit_uint GROUP BY k_u128 ORDER BY k_u128 ASC LIMIT 10;
SET enable_group_by_top_k_optimization = 0;
INSERT INTO gt_keys128
SELECT k_u128, count(), sum(val)
FROM t_gbylimit_uint GROUP BY k_u128 ORDER BY k_u128 ASC LIMIT 10;
SET enable_group_by_top_k_optimization = 1;

SELECT 'keys128';
SELECT k_u128, count(), sum(val)
FROM t_gbylimit_uint GROUP BY k_u128 ORDER BY k_u128 ASC LIMIT 10
EXCEPT
SELECT * FROM gt_keys128;
SELECT * FROM (SELECT * FROM gt_keys128)
EXCEPT
SELECT * FROM (SELECT k_u128, count(), sum(val)
FROM t_gbylimit_uint GROUP BY k_u128 ORDER BY k_u128 ASC LIMIT 10);

DROP TABLE IF EXISTS gt_keys256;
CREATE TABLE gt_keys256 ENGINE = Memory EMPTY AS
SELECT k_u256, count(), sum(val)
FROM t_gbylimit_uint GROUP BY k_u256 ORDER BY k_u256 ASC LIMIT 10;
SET enable_group_by_top_k_optimization = 0;
INSERT INTO gt_keys256
SELECT k_u256, count(), sum(val)
FROM t_gbylimit_uint GROUP BY k_u256 ORDER BY k_u256 ASC LIMIT 10;
SET enable_group_by_top_k_optimization = 1;

SELECT 'keys256';
SELECT k_u256, count(), sum(val)
FROM t_gbylimit_uint GROUP BY k_u256 ORDER BY k_u256 ASC LIMIT 10
EXCEPT
SELECT * FROM gt_keys256;
SELECT * FROM (SELECT * FROM gt_keys256)
EXCEPT
SELECT * FROM (SELECT k_u256, count(), sum(val)
FROM t_gbylimit_uint GROUP BY k_u256 ORDER BY k_u256 ASC LIMIT 10);

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

DROP TABLE IF EXISTS gt_int8_asc;
CREATE TABLE gt_int8_asc ENGINE = Memory EMPTY AS
SELECT k_i8, count(), sum(val)
FROM t_gbylimit_signed GROUP BY k_i8 ORDER BY k_i8 ASC LIMIT 10;
SET enable_group_by_top_k_optimization = 0;
INSERT INTO gt_int8_asc
SELECT k_i8, count(), sum(val)
FROM t_gbylimit_signed GROUP BY k_i8 ORDER BY k_i8 ASC LIMIT 10;
SET enable_group_by_top_k_optimization = 1;

SELECT 'int8_asc';
SELECT k_i8, count(), sum(val)
FROM t_gbylimit_signed GROUP BY k_i8 ORDER BY k_i8 ASC LIMIT 10
EXCEPT
SELECT * FROM gt_int8_asc;
SELECT * FROM (SELECT * FROM gt_int8_asc)
EXCEPT
SELECT * FROM (SELECT k_i8, count(), sum(val)
FROM t_gbylimit_signed GROUP BY k_i8 ORDER BY k_i8 ASC LIMIT 10);

DROP TABLE IF EXISTS gt_int8_desc;
CREATE TABLE gt_int8_desc ENGINE = Memory EMPTY AS
SELECT k_i8, count(), sum(val)
FROM t_gbylimit_signed GROUP BY k_i8 ORDER BY k_i8 DESC LIMIT 10;
SET enable_group_by_top_k_optimization = 0;
INSERT INTO gt_int8_desc
SELECT k_i8, count(), sum(val)
FROM t_gbylimit_signed GROUP BY k_i8 ORDER BY k_i8 DESC LIMIT 10;
SET enable_group_by_top_k_optimization = 1;

SELECT 'int8_desc';
SELECT k_i8, count(), sum(val)
FROM t_gbylimit_signed GROUP BY k_i8 ORDER BY k_i8 DESC LIMIT 10
EXCEPT
SELECT * FROM gt_int8_desc;
SELECT * FROM (SELECT * FROM gt_int8_desc)
EXCEPT
SELECT * FROM (SELECT k_i8, count(), sum(val)
FROM t_gbylimit_signed GROUP BY k_i8 ORDER BY k_i8 DESC LIMIT 10);

DROP TABLE IF EXISTS gt_int16_asc;
CREATE TABLE gt_int16_asc ENGINE = Memory EMPTY AS
SELECT k_i16, count(), sum(val)
FROM t_gbylimit_signed GROUP BY k_i16 ORDER BY k_i16 ASC LIMIT 10;
SET enable_group_by_top_k_optimization = 0;
INSERT INTO gt_int16_asc
SELECT k_i16, count(), sum(val)
FROM t_gbylimit_signed GROUP BY k_i16 ORDER BY k_i16 ASC LIMIT 10;
SET enable_group_by_top_k_optimization = 1;

SELECT 'int16_asc';
SELECT k_i16, count(), sum(val)
FROM t_gbylimit_signed GROUP BY k_i16 ORDER BY k_i16 ASC LIMIT 10
EXCEPT
SELECT * FROM gt_int16_asc;
SELECT * FROM (SELECT * FROM gt_int16_asc)
EXCEPT
SELECT * FROM (SELECT k_i16, count(), sum(val)
FROM t_gbylimit_signed GROUP BY k_i16 ORDER BY k_i16 ASC LIMIT 10);

DROP TABLE IF EXISTS gt_int16_desc;
CREATE TABLE gt_int16_desc ENGINE = Memory EMPTY AS
SELECT k_i16, count(), sum(val)
FROM t_gbylimit_signed GROUP BY k_i16 ORDER BY k_i16 DESC LIMIT 10;
SET enable_group_by_top_k_optimization = 0;
INSERT INTO gt_int16_desc
SELECT k_i16, count(), sum(val)
FROM t_gbylimit_signed GROUP BY k_i16 ORDER BY k_i16 DESC LIMIT 10;
SET enable_group_by_top_k_optimization = 1;

SELECT 'int16_desc';
SELECT k_i16, count(), sum(val)
FROM t_gbylimit_signed GROUP BY k_i16 ORDER BY k_i16 DESC LIMIT 10
EXCEPT
SELECT * FROM gt_int16_desc;
SELECT * FROM (SELECT * FROM gt_int16_desc)
EXCEPT
SELECT * FROM (SELECT k_i16, count(), sum(val)
FROM t_gbylimit_signed GROUP BY k_i16 ORDER BY k_i16 DESC LIMIT 10);

DROP TABLE IF EXISTS gt_int32_asc;
CREATE TABLE gt_int32_asc ENGINE = Memory EMPTY AS
SELECT k_i32, count(), sum(val)
FROM t_gbylimit_signed GROUP BY k_i32 ORDER BY k_i32 ASC LIMIT 10;
SET enable_group_by_top_k_optimization = 0;
INSERT INTO gt_int32_asc
SELECT k_i32, count(), sum(val)
FROM t_gbylimit_signed GROUP BY k_i32 ORDER BY k_i32 ASC LIMIT 10;
SET enable_group_by_top_k_optimization = 1;

SELECT 'int32_asc';
SELECT k_i32, count(), sum(val)
FROM t_gbylimit_signed GROUP BY k_i32 ORDER BY k_i32 ASC LIMIT 10
EXCEPT
SELECT * FROM gt_int32_asc;
SELECT * FROM (SELECT * FROM gt_int32_asc)
EXCEPT
SELECT * FROM (SELECT k_i32, count(), sum(val)
FROM t_gbylimit_signed GROUP BY k_i32 ORDER BY k_i32 ASC LIMIT 10);

DROP TABLE IF EXISTS gt_int32_desc;
CREATE TABLE gt_int32_desc ENGINE = Memory EMPTY AS
SELECT k_i32, count(), sum(val)
FROM t_gbylimit_signed GROUP BY k_i32 ORDER BY k_i32 DESC LIMIT 10;
SET enable_group_by_top_k_optimization = 0;
INSERT INTO gt_int32_desc
SELECT k_i32, count(), sum(val)
FROM t_gbylimit_signed GROUP BY k_i32 ORDER BY k_i32 DESC LIMIT 10;
SET enable_group_by_top_k_optimization = 1;

SELECT 'int32_desc';
SELECT k_i32, count(), sum(val)
FROM t_gbylimit_signed GROUP BY k_i32 ORDER BY k_i32 DESC LIMIT 10
EXCEPT
SELECT * FROM gt_int32_desc;
SELECT * FROM (SELECT * FROM gt_int32_desc)
EXCEPT
SELECT * FROM (SELECT k_i32, count(), sum(val)
FROM t_gbylimit_signed GROUP BY k_i32 ORDER BY k_i32 DESC LIMIT 10);

DROP TABLE IF EXISTS gt_int64_asc;
CREATE TABLE gt_int64_asc ENGINE = Memory EMPTY AS
SELECT k_i64, count(), sum(val)
FROM t_gbylimit_signed GROUP BY k_i64 ORDER BY k_i64 ASC LIMIT 10;
SET enable_group_by_top_k_optimization = 0;
INSERT INTO gt_int64_asc
SELECT k_i64, count(), sum(val)
FROM t_gbylimit_signed GROUP BY k_i64 ORDER BY k_i64 ASC LIMIT 10;
SET enable_group_by_top_k_optimization = 1;

SELECT 'int64_asc';
SELECT k_i64, count(), sum(val)
FROM t_gbylimit_signed GROUP BY k_i64 ORDER BY k_i64 ASC LIMIT 10
EXCEPT
SELECT * FROM gt_int64_asc;
SELECT * FROM (SELECT * FROM gt_int64_asc)
EXCEPT
SELECT * FROM (SELECT k_i64, count(), sum(val)
FROM t_gbylimit_signed GROUP BY k_i64 ORDER BY k_i64 ASC LIMIT 10);

DROP TABLE IF EXISTS gt_int64_desc;
CREATE TABLE gt_int64_desc ENGINE = Memory EMPTY AS
SELECT k_i64, count(), sum(val)
FROM t_gbylimit_signed GROUP BY k_i64 ORDER BY k_i64 DESC LIMIT 10;
SET enable_group_by_top_k_optimization = 0;
INSERT INTO gt_int64_desc
SELECT k_i64, count(), sum(val)
FROM t_gbylimit_signed GROUP BY k_i64 ORDER BY k_i64 DESC LIMIT 10;
SET enable_group_by_top_k_optimization = 1;

SELECT 'int64_desc';
SELECT k_i64, count(), sum(val)
FROM t_gbylimit_signed GROUP BY k_i64 ORDER BY k_i64 DESC LIMIT 10
EXCEPT
SELECT * FROM gt_int64_desc;
SELECT * FROM (SELECT * FROM gt_int64_desc)
EXCEPT
SELECT * FROM (SELECT k_i64, count(), sum(val)
FROM t_gbylimit_signed GROUP BY k_i64 ORDER BY k_i64 DESC LIMIT 10);

DROP TABLE IF EXISTS gt_date32_asc;
CREATE TABLE gt_date32_asc ENGINE = Memory EMPTY AS
SELECT k_d32, count(), sum(val)
FROM t_gbylimit_signed GROUP BY k_d32 ORDER BY k_d32 ASC LIMIT 10;
SET enable_group_by_top_k_optimization = 0;
INSERT INTO gt_date32_asc
SELECT k_d32, count(), sum(val)
FROM t_gbylimit_signed GROUP BY k_d32 ORDER BY k_d32 ASC LIMIT 10;
SET enable_group_by_top_k_optimization = 1;

SELECT 'date32_asc';
SELECT k_d32, count(), sum(val)
FROM t_gbylimit_signed GROUP BY k_d32 ORDER BY k_d32 ASC LIMIT 10
EXCEPT
SELECT * FROM gt_date32_asc;
SELECT * FROM (SELECT * FROM gt_date32_asc)
EXCEPT
SELECT * FROM (SELECT k_d32, count(), sum(val)
FROM t_gbylimit_signed GROUP BY k_d32 ORDER BY k_d32 ASC LIMIT 10);

DROP TABLE IF EXISTS gt_date32_desc;
CREATE TABLE gt_date32_desc ENGINE = Memory EMPTY AS
SELECT k_d32, count(), sum(val)
FROM t_gbylimit_signed GROUP BY k_d32 ORDER BY k_d32 DESC LIMIT 10;
SET enable_group_by_top_k_optimization = 0;
INSERT INTO gt_date32_desc
SELECT k_d32, count(), sum(val)
FROM t_gbylimit_signed GROUP BY k_d32 ORDER BY k_d32 DESC LIMIT 10;
SET enable_group_by_top_k_optimization = 1;

SELECT 'date32_desc';
SELECT k_d32, count(), sum(val)
FROM t_gbylimit_signed GROUP BY k_d32 ORDER BY k_d32 DESC LIMIT 10
EXCEPT
SELECT * FROM gt_date32_desc;
SELECT * FROM (SELECT * FROM gt_date32_desc)
EXCEPT
SELECT * FROM (SELECT k_d32, count(), sum(val)
FROM t_gbylimit_signed GROUP BY k_d32 ORDER BY k_d32 DESC LIMIT 10);

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

DROP TABLE IF EXISTS gt_float32_asc;
CREATE TABLE gt_float32_asc ENGINE = Memory EMPTY AS
SELECT k_f32, count(), sum(val)
FROM t_gbylimit_float GROUP BY k_f32 ORDER BY k_f32 ASC LIMIT 10;
SET enable_group_by_top_k_optimization = 0;
INSERT INTO gt_float32_asc
SELECT k_f32, count(), sum(val)
FROM t_gbylimit_float GROUP BY k_f32 ORDER BY k_f32 ASC LIMIT 10;
SET enable_group_by_top_k_optimization = 1;

SELECT 'float32_asc';
SELECT k_f32, count(), sum(val)
FROM t_gbylimit_float GROUP BY k_f32 ORDER BY k_f32 ASC LIMIT 10
EXCEPT
SELECT * FROM gt_float32_asc;
SELECT * FROM (SELECT * FROM gt_float32_asc)
EXCEPT
SELECT * FROM (SELECT k_f32, count(), sum(val)
FROM t_gbylimit_float GROUP BY k_f32 ORDER BY k_f32 ASC LIMIT 10);

DROP TABLE IF EXISTS gt_float32_desc;
CREATE TABLE gt_float32_desc ENGINE = Memory EMPTY AS
SELECT k_f32, count(), sum(val)
FROM t_gbylimit_float GROUP BY k_f32 ORDER BY k_f32 DESC LIMIT 10;
SET enable_group_by_top_k_optimization = 0;
INSERT INTO gt_float32_desc
SELECT k_f32, count(), sum(val)
FROM t_gbylimit_float GROUP BY k_f32 ORDER BY k_f32 DESC LIMIT 10;
SET enable_group_by_top_k_optimization = 1;

SELECT 'float32_desc';
SELECT k_f32, count(), sum(val)
FROM t_gbylimit_float GROUP BY k_f32 ORDER BY k_f32 DESC LIMIT 10
EXCEPT
SELECT * FROM gt_float32_desc;
SELECT * FROM (SELECT * FROM gt_float32_desc)
EXCEPT
SELECT * FROM (SELECT k_f32, count(), sum(val)
FROM t_gbylimit_float GROUP BY k_f32 ORDER BY k_f32 DESC LIMIT 10);

DROP TABLE IF EXISTS gt_float64_asc;
CREATE TABLE gt_float64_asc ENGINE = Memory EMPTY AS
SELECT k_f64, count(), sum(val)
FROM t_gbylimit_float GROUP BY k_f64 ORDER BY k_f64 ASC LIMIT 10;
SET enable_group_by_top_k_optimization = 0;
INSERT INTO gt_float64_asc
SELECT k_f64, count(), sum(val)
FROM t_gbylimit_float GROUP BY k_f64 ORDER BY k_f64 ASC LIMIT 10;
SET enable_group_by_top_k_optimization = 1;

SELECT 'float64_asc';
SELECT k_f64, count(), sum(val)
FROM t_gbylimit_float GROUP BY k_f64 ORDER BY k_f64 ASC LIMIT 10
EXCEPT
SELECT * FROM gt_float64_asc;
SELECT * FROM (SELECT * FROM gt_float64_asc)
EXCEPT
SELECT * FROM (SELECT k_f64, count(), sum(val)
FROM t_gbylimit_float GROUP BY k_f64 ORDER BY k_f64 ASC LIMIT 10);

DROP TABLE IF EXISTS gt_float64_desc;
CREATE TABLE gt_float64_desc ENGINE = Memory EMPTY AS
SELECT k_f64, count(), sum(val)
FROM t_gbylimit_float GROUP BY k_f64 ORDER BY k_f64 DESC LIMIT 10;
SET enable_group_by_top_k_optimization = 0;
INSERT INTO gt_float64_desc
SELECT k_f64, count(), sum(val)
FROM t_gbylimit_float GROUP BY k_f64 ORDER BY k_f64 DESC LIMIT 10;
SET enable_group_by_top_k_optimization = 1;

SELECT 'float64_desc';
SELECT k_f64, count(), sum(val)
FROM t_gbylimit_float GROUP BY k_f64 ORDER BY k_f64 DESC LIMIT 10
EXCEPT
SELECT * FROM gt_float64_desc;
SELECT * FROM (SELECT * FROM gt_float64_desc)
EXCEPT
SELECT * FROM (SELECT k_f64, count(), sum(val)
FROM t_gbylimit_float GROUP BY k_f64 ORDER BY k_f64 DESC LIMIT 10);

DROP TABLE IF EXISTS gt_float32_asc_nulls_first;
CREATE TABLE gt_float32_asc_nulls_first ENGINE = Memory EMPTY AS
SELECT k_f32, count(), sum(val)
FROM t_gbylimit_float GROUP BY k_f32 ORDER BY k_f32 ASC NULLS FIRST LIMIT 10;
SET enable_group_by_top_k_optimization = 0;
INSERT INTO gt_float32_asc_nulls_first
SELECT k_f32, count(), sum(val)
FROM t_gbylimit_float GROUP BY k_f32 ORDER BY k_f32 ASC NULLS FIRST LIMIT 10;
SET enable_group_by_top_k_optimization = 1;

SELECT 'float32_asc_nulls_first';
SELECT k_f32, count(), sum(val)
FROM t_gbylimit_float GROUP BY k_f32 ORDER BY k_f32 ASC NULLS FIRST LIMIT 10
EXCEPT
SELECT * FROM gt_float32_asc_nulls_first;
SELECT * FROM (SELECT * FROM gt_float32_asc_nulls_first)
EXCEPT
SELECT * FROM (SELECT k_f32, count(), sum(val)
FROM t_gbylimit_float GROUP BY k_f32 ORDER BY k_f32 ASC NULLS FIRST LIMIT 10);

DROP TABLE IF EXISTS gt_float32_asc_nulls_last;
CREATE TABLE gt_float32_asc_nulls_last ENGINE = Memory EMPTY AS
SELECT k_f32, count(), sum(val)
FROM t_gbylimit_float GROUP BY k_f32 ORDER BY k_f32 ASC NULLS LAST LIMIT 10;
SET enable_group_by_top_k_optimization = 0;
INSERT INTO gt_float32_asc_nulls_last
SELECT k_f32, count(), sum(val)
FROM t_gbylimit_float GROUP BY k_f32 ORDER BY k_f32 ASC NULLS LAST LIMIT 10;
SET enable_group_by_top_k_optimization = 1;

SELECT 'float32_asc_nulls_last';
SELECT k_f32, count(), sum(val)
FROM t_gbylimit_float GROUP BY k_f32 ORDER BY k_f32 ASC NULLS LAST LIMIT 10
EXCEPT
SELECT * FROM gt_float32_asc_nulls_last;
SELECT * FROM (SELECT * FROM gt_float32_asc_nulls_last)
EXCEPT
SELECT * FROM (SELECT k_f32, count(), sum(val)
FROM t_gbylimit_float GROUP BY k_f32 ORDER BY k_f32 ASC NULLS LAST LIMIT 10);

DROP TABLE IF EXISTS gt_float32_desc_nulls_first;
CREATE TABLE gt_float32_desc_nulls_first ENGINE = Memory EMPTY AS
SELECT k_f32, count(), sum(val)
FROM t_gbylimit_float GROUP BY k_f32 ORDER BY k_f32 DESC NULLS FIRST LIMIT 10;
SET enable_group_by_top_k_optimization = 0;
INSERT INTO gt_float32_desc_nulls_first
SELECT k_f32, count(), sum(val)
FROM t_gbylimit_float GROUP BY k_f32 ORDER BY k_f32 DESC NULLS FIRST LIMIT 10;
SET enable_group_by_top_k_optimization = 1;

SELECT 'float32_desc_nulls_first';
SELECT k_f32, count(), sum(val)
FROM t_gbylimit_float GROUP BY k_f32 ORDER BY k_f32 DESC NULLS FIRST LIMIT 10
EXCEPT
SELECT * FROM gt_float32_desc_nulls_first;
SELECT * FROM (SELECT * FROM gt_float32_desc_nulls_first)
EXCEPT
SELECT * FROM (SELECT k_f32, count(), sum(val)
FROM t_gbylimit_float GROUP BY k_f32 ORDER BY k_f32 DESC NULLS FIRST LIMIT 10);

DROP TABLE IF EXISTS gt_float32_desc_nulls_last;
CREATE TABLE gt_float32_desc_nulls_last ENGINE = Memory EMPTY AS
SELECT k_f32, count(), sum(val)
FROM t_gbylimit_float GROUP BY k_f32 ORDER BY k_f32 DESC NULLS LAST LIMIT 10;
SET enable_group_by_top_k_optimization = 0;
INSERT INTO gt_float32_desc_nulls_last
SELECT k_f32, count(), sum(val)
FROM t_gbylimit_float GROUP BY k_f32 ORDER BY k_f32 DESC NULLS LAST LIMIT 10;
SET enable_group_by_top_k_optimization = 1;

SELECT 'float32_desc_nulls_last';
SELECT k_f32, count(), sum(val)
FROM t_gbylimit_float GROUP BY k_f32 ORDER BY k_f32 DESC NULLS LAST LIMIT 10
EXCEPT
SELECT * FROM gt_float32_desc_nulls_last;
SELECT * FROM (SELECT * FROM gt_float32_desc_nulls_last)
EXCEPT
SELECT * FROM (SELECT k_f32, count(), sum(val)
FROM t_gbylimit_float GROUP BY k_f32 ORDER BY k_f32 DESC NULLS LAST LIMIT 10);

DROP TABLE IF EXISTS gt_float64_asc_nulls_first;
CREATE TABLE gt_float64_asc_nulls_first ENGINE = Memory EMPTY AS
SELECT k_f64, count(), sum(val)
FROM t_gbylimit_float GROUP BY k_f64 ORDER BY k_f64 ASC NULLS FIRST LIMIT 10;
SET enable_group_by_top_k_optimization = 0;
INSERT INTO gt_float64_asc_nulls_first
SELECT k_f64, count(), sum(val)
FROM t_gbylimit_float GROUP BY k_f64 ORDER BY k_f64 ASC NULLS FIRST LIMIT 10;
SET enable_group_by_top_k_optimization = 1;

SELECT 'float64_asc_nulls_first';
SELECT k_f64, count(), sum(val)
FROM t_gbylimit_float GROUP BY k_f64 ORDER BY k_f64 ASC NULLS FIRST LIMIT 10
EXCEPT
SELECT * FROM gt_float64_asc_nulls_first;
SELECT * FROM (SELECT * FROM gt_float64_asc_nulls_first)
EXCEPT
SELECT * FROM (SELECT k_f64, count(), sum(val)
FROM t_gbylimit_float GROUP BY k_f64 ORDER BY k_f64 ASC NULLS FIRST LIMIT 10);

DROP TABLE IF EXISTS gt_float64_desc_nulls_last;
CREATE TABLE gt_float64_desc_nulls_last ENGINE = Memory EMPTY AS
SELECT k_f64, count(), sum(val)
FROM t_gbylimit_float GROUP BY k_f64 ORDER BY k_f64 DESC NULLS LAST LIMIT 10;
SET enable_group_by_top_k_optimization = 0;
INSERT INTO gt_float64_desc_nulls_last
SELECT k_f64, count(), sum(val)
FROM t_gbylimit_float GROUP BY k_f64 ORDER BY k_f64 DESC NULLS LAST LIMIT 10;
SET enable_group_by_top_k_optimization = 1;

SELECT 'float64_desc_nulls_last';
SELECT k_f64, count(), sum(val)
FROM t_gbylimit_float GROUP BY k_f64 ORDER BY k_f64 DESC NULLS LAST LIMIT 10
EXCEPT
SELECT * FROM gt_float64_desc_nulls_last;
SELECT * FROM (SELECT * FROM gt_float64_desc_nulls_last)
EXCEPT
SELECT * FROM (SELECT k_f64, count(), sum(val)
FROM t_gbylimit_float GROUP BY k_f64 ORDER BY k_f64 DESC NULLS LAST LIMIT 10);

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

DROP TABLE IF EXISTS gt_datetime_asc;
CREATE TABLE gt_datetime_asc ENGINE = Memory EMPTY AS
SELECT k_dt, count(), sum(val)
FROM t_gbylimit_datetime GROUP BY k_dt ORDER BY k_dt ASC LIMIT 10;
SET enable_group_by_top_k_optimization = 0;
INSERT INTO gt_datetime_asc
SELECT k_dt, count(), sum(val)
FROM t_gbylimit_datetime GROUP BY k_dt ORDER BY k_dt ASC LIMIT 10;
SET enable_group_by_top_k_optimization = 1;

SELECT 'datetime_asc';
SELECT k_dt, count(), sum(val)
FROM t_gbylimit_datetime GROUP BY k_dt ORDER BY k_dt ASC LIMIT 10
EXCEPT
SELECT * FROM gt_datetime_asc;
SELECT * FROM (SELECT * FROM gt_datetime_asc)
EXCEPT
SELECT * FROM (SELECT k_dt, count(), sum(val)
FROM t_gbylimit_datetime GROUP BY k_dt ORDER BY k_dt ASC LIMIT 10);

DROP TABLE IF EXISTS gt_datetime_desc;
CREATE TABLE gt_datetime_desc ENGINE = Memory EMPTY AS
SELECT k_dt, count(), sum(val)
FROM t_gbylimit_datetime GROUP BY k_dt ORDER BY k_dt DESC LIMIT 10;
SET enable_group_by_top_k_optimization = 0;
INSERT INTO gt_datetime_desc
SELECT k_dt, count(), sum(val)
FROM t_gbylimit_datetime GROUP BY k_dt ORDER BY k_dt DESC LIMIT 10;
SET enable_group_by_top_k_optimization = 1;

SELECT 'datetime_desc';
SELECT k_dt, count(), sum(val)
FROM t_gbylimit_datetime GROUP BY k_dt ORDER BY k_dt DESC LIMIT 10
EXCEPT
SELECT * FROM gt_datetime_desc;
SELECT * FROM (SELECT * FROM gt_datetime_desc)
EXCEPT
SELECT * FROM (SELECT k_dt, count(), sum(val)
FROM t_gbylimit_datetime GROUP BY k_dt ORDER BY k_dt DESC LIMIT 10);

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

DROP TABLE IF EXISTS gt_key_string;
CREATE TABLE gt_key_string ENGINE = Memory EMPTY AS
SELECT k_str, count(), sum(val)
FROM t_gbylimit_str GROUP BY k_str ORDER BY k_str ASC LIMIT 10;
SET enable_group_by_top_k_optimization = 0;
INSERT INTO gt_key_string
SELECT k_str, count(), sum(val)
FROM t_gbylimit_str GROUP BY k_str ORDER BY k_str ASC LIMIT 10;
SET enable_group_by_top_k_optimization = 1;

SELECT 'key_string';
SELECT k_str, count(), sum(val)
FROM t_gbylimit_str GROUP BY k_str ORDER BY k_str ASC LIMIT 10
EXCEPT
SELECT * FROM gt_key_string;
SELECT * FROM (SELECT * FROM gt_key_string)
EXCEPT
SELECT * FROM (SELECT k_str, count(), sum(val)
FROM t_gbylimit_str GROUP BY k_str ORDER BY k_str ASC LIMIT 10);

DROP TABLE IF EXISTS gt_key_fixed_string;
CREATE TABLE gt_key_fixed_string ENGINE = Memory EMPTY AS
SELECT k_fstr, count(), sum(val)
FROM t_gbylimit_str GROUP BY k_fstr ORDER BY k_fstr ASC LIMIT 10;
SET enable_group_by_top_k_optimization = 0;
INSERT INTO gt_key_fixed_string
SELECT k_fstr, count(), sum(val)
FROM t_gbylimit_str GROUP BY k_fstr ORDER BY k_fstr ASC LIMIT 10;
SET enable_group_by_top_k_optimization = 1;

SELECT 'key_fixed_string';
SELECT k_fstr, count(), sum(val)
FROM t_gbylimit_str GROUP BY k_fstr ORDER BY k_fstr ASC LIMIT 10
EXCEPT
SELECT * FROM gt_key_fixed_string;
SELECT * FROM (SELECT * FROM gt_key_fixed_string)
EXCEPT
SELECT * FROM (SELECT k_fstr, count(), sum(val)
FROM t_gbylimit_str GROUP BY k_fstr ORDER BY k_fstr ASC LIMIT 10);

DROP TABLE IF EXISTS gt_serialized;
CREATE TABLE gt_serialized ENGINE = Memory EMPTY AS
SELECT k_tup, count(), sum(val)
FROM t_gbylimit_str GROUP BY k_tup ORDER BY k_tup ASC LIMIT 10;
SET enable_group_by_top_k_optimization = 0;
INSERT INTO gt_serialized
SELECT k_tup, count(), sum(val)
FROM t_gbylimit_str GROUP BY k_tup ORDER BY k_tup ASC LIMIT 10;
SET enable_group_by_top_k_optimization = 1;

SELECT 'serialized';
SELECT k_tup, count(), sum(val)
FROM t_gbylimit_str GROUP BY k_tup ORDER BY k_tup ASC LIMIT 10
EXCEPT
SELECT * FROM gt_serialized;
SELECT * FROM (SELECT * FROM gt_serialized)
EXCEPT
SELECT * FROM (SELECT k_tup, count(), sum(val)
FROM t_gbylimit_str GROUP BY k_tup ORDER BY k_tup ASC LIMIT 10);

DROP TABLE IF EXISTS gt_nullable_key32;
CREATE TABLE gt_nullable_key32 ENGINE = Memory EMPTY AS
SELECT k_nu32, count(), sum(val)
FROM t_gbylimit_str GROUP BY k_nu32 ORDER BY k_nu32 ASC LIMIT 10;
SET enable_group_by_top_k_optimization = 0;
INSERT INTO gt_nullable_key32
SELECT k_nu32, count(), sum(val)
FROM t_gbylimit_str GROUP BY k_nu32 ORDER BY k_nu32 ASC LIMIT 10;
SET enable_group_by_top_k_optimization = 1;

SELECT 'nullable_key32';
SELECT k_nu32, count(), sum(val)
FROM t_gbylimit_str GROUP BY k_nu32 ORDER BY k_nu32 ASC LIMIT 10
EXCEPT
SELECT * FROM gt_nullable_key32;
SELECT * FROM (SELECT * FROM gt_nullable_key32)
EXCEPT
SELECT * FROM (SELECT k_nu32, count(), sum(val)
FROM t_gbylimit_str GROUP BY k_nu32 ORDER BY k_nu32 ASC LIMIT 10);

DROP TABLE IF EXISTS gt_nullable_key_string;
CREATE TABLE gt_nullable_key_string ENGINE = Memory EMPTY AS
SELECT k_nstr, count(), sum(val)
FROM t_gbylimit_str GROUP BY k_nstr ORDER BY k_nstr ASC LIMIT 10;
SET enable_group_by_top_k_optimization = 0;
INSERT INTO gt_nullable_key_string
SELECT k_nstr, count(), sum(val)
FROM t_gbylimit_str GROUP BY k_nstr ORDER BY k_nstr ASC LIMIT 10;
SET enable_group_by_top_k_optimization = 1;

SELECT 'nullable_key_string';
SELECT k_nstr, count(), sum(val)
FROM t_gbylimit_str GROUP BY k_nstr ORDER BY k_nstr ASC LIMIT 10
EXCEPT
SELECT * FROM gt_nullable_key_string;
SELECT * FROM (SELECT * FROM gt_nullable_key_string)
EXCEPT
SELECT * FROM (SELECT k_nstr, count(), sum(val)
FROM t_gbylimit_str GROUP BY k_nstr ORDER BY k_nstr ASC LIMIT 10);

DROP TABLE IF EXISTS gt_low_cardinality_key64;
CREATE TABLE gt_low_cardinality_key64 ENGINE = Memory EMPTY AS
SELECT k_lcu64, count(), sum(val)
FROM t_gbylimit_str GROUP BY k_lcu64 ORDER BY k_lcu64 ASC LIMIT 10;
SET enable_group_by_top_k_optimization = 0;
INSERT INTO gt_low_cardinality_key64
SELECT k_lcu64, count(), sum(val)
FROM t_gbylimit_str GROUP BY k_lcu64 ORDER BY k_lcu64 ASC LIMIT 10;
SET enable_group_by_top_k_optimization = 1;

SELECT 'low_cardinality_key64';
SELECT k_lcu64, count(), sum(val)
FROM t_gbylimit_str GROUP BY k_lcu64 ORDER BY k_lcu64 ASC LIMIT 10
EXCEPT
SELECT * FROM gt_low_cardinality_key64;
SELECT * FROM (SELECT * FROM gt_low_cardinality_key64)
EXCEPT
SELECT * FROM (SELECT k_lcu64, count(), sum(val)
FROM t_gbylimit_str GROUP BY k_lcu64 ORDER BY k_lcu64 ASC LIMIT 10);

DROP TABLE IF EXISTS gt_low_cardinality_key_string;
CREATE TABLE gt_low_cardinality_key_string ENGINE = Memory EMPTY AS
SELECT k_lcstr, count(), sum(val)
FROM t_gbylimit_str GROUP BY k_lcstr ORDER BY k_lcstr ASC LIMIT 10;
SET enable_group_by_top_k_optimization = 0;
INSERT INTO gt_low_cardinality_key_string
SELECT k_lcstr, count(), sum(val)
FROM t_gbylimit_str GROUP BY k_lcstr ORDER BY k_lcstr ASC LIMIT 10;
SET enable_group_by_top_k_optimization = 1;

SELECT 'low_cardinality_key_string';
SELECT k_lcstr, count(), sum(val)
FROM t_gbylimit_str GROUP BY k_lcstr ORDER BY k_lcstr ASC LIMIT 10
EXCEPT
SELECT * FROM gt_low_cardinality_key_string;
SELECT * FROM (SELECT * FROM gt_low_cardinality_key_string)
EXCEPT
SELECT * FROM (SELECT k_lcstr, count(), sum(val)
FROM t_gbylimit_str GROUP BY k_lcstr ORDER BY k_lcstr ASC LIMIT 10);

DROP TABLE t_gbylimit_str;

SELECT 'optimization_applied_guard';
SELECT count() FROM (EXPLAIN actions = 1 SELECT number AS k FROM numbers(100) GROUP BY k ORDER BY k LIMIT 5) WHERE explain LIKE '%Top-K%';

SELECT 'per_family_engagement';
SELECT k, count() FROM (SELECT toUInt32(99999 - number) AS k FROM numbers(100000)) GROUP BY k ORDER BY k ASC LIMIT 10 SETTINGS log_comment = '04498_engage_key32' FORMAT Null;
SELECT k, count() FROM (SELECT toUInt64(99999 - number) AS k FROM numbers(100000)) GROUP BY k ORDER BY k ASC LIMIT 10 SETTINGS log_comment = '04498_engage_key64' FORMAT Null;
SELECT a, b, count() FROM (SELECT toUInt64(99999 - number) AS a, toUInt64(number % 3) AS b FROM numbers(100000)) GROUP BY a, b ORDER BY a ASC, b ASC LIMIT 10 SETTINGS log_comment = '04498_engage_keys128' FORMAT Null;
SELECT a, b, c, d, count() FROM (SELECT toUInt64(99999 - number) AS a, toUInt64(number % 3) AS b, toUInt64(number % 5) AS c, toUInt64(number % 7) AS d FROM numbers(100000)) GROUP BY a, b, c, d ORDER BY a ASC, b ASC, c ASC, d ASC LIMIT 10 SETTINGS log_comment = '04498_engage_keys256' FORMAT Null;
SELECT k, count() FROM (SELECT toFloat64(99999 - number) AS k FROM numbers(100000)) GROUP BY k ORDER BY k ASC LIMIT 10 SETTINGS log_comment = '04498_engage_float64' FORMAT Null;
SELECT k, count() FROM (SELECT leftPad(toString(99999 - number), 6, '0') AS k FROM numbers(100000)) GROUP BY k ORDER BY k ASC LIMIT 10 SETTINGS log_comment = '04498_engage_string' FORMAT Null;
SELECT k, count() FROM (SELECT toFixedString(leftPad(toString(99999 - number), 6, '0'), 6) AS k FROM numbers(100000)) GROUP BY k ORDER BY k ASC LIMIT 10 SETTINGS log_comment = '04498_engage_fixed_string' FORMAT Null;
SELECT a, b, count() FROM (SELECT leftPad(toString(99999 - number), 6, '0') AS a, toUInt32(number % 3) AS b FROM numbers(100000)) GROUP BY a, b ORDER BY a ASC, b ASC LIMIT 10 SETTINGS log_comment = '04498_engage_serialized' FORMAT Null;
SELECT k, count() FROM (SELECT if(number % 100 = 7, NULL, toNullable(toUInt32(99999 - number))) AS k FROM numbers(100000)) GROUP BY k ORDER BY k ASC NULLS LAST LIMIT 10 SETTINGS log_comment = '04498_engage_nullable' FORMAT Null;
SELECT k, count() FROM (SELECT toLowCardinality(leftPad(toString(9999 - (number % 10000)), 5, '0')) AS k FROM numbers(100000)) GROUP BY k ORDER BY k ASC LIMIT 10 SETTINGS log_comment = '04498_engage_low_cardinality' FORMAT Null;
SYSTEM FLUSH LOGS query_log;
SELECT log_comment, max(ProfileEvents['AggregationTopKRowsSkipped'] + ProfileEvents['AggregationTopKKeysEvicted']) > 0
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment LIKE '04498_engage_%'
GROUP BY log_comment ORDER BY log_comment;

DROP TABLE gt_key8;
DROP TABLE gt_key16;
DROP TABLE gt_key32;
DROP TABLE gt_key64;
DROP TABLE gt_keys128;
DROP TABLE gt_keys256;
DROP TABLE gt_int8_asc;
DROP TABLE gt_int8_desc;
DROP TABLE gt_int16_asc;
DROP TABLE gt_int16_desc;
DROP TABLE gt_int32_asc;
DROP TABLE gt_int32_desc;
DROP TABLE gt_int64_asc;
DROP TABLE gt_int64_desc;
DROP TABLE gt_date32_asc;
DROP TABLE gt_date32_desc;
DROP TABLE gt_float32_asc;
DROP TABLE gt_float32_desc;
DROP TABLE gt_float64_asc;
DROP TABLE gt_float64_desc;
DROP TABLE gt_float32_asc_nulls_first;
DROP TABLE gt_float32_asc_nulls_last;
DROP TABLE gt_float32_desc_nulls_first;
DROP TABLE gt_float32_desc_nulls_last;
DROP TABLE gt_float64_asc_nulls_first;
DROP TABLE gt_float64_desc_nulls_last;
DROP TABLE gt_datetime_asc;
DROP TABLE gt_datetime_desc;
DROP TABLE gt_key_string;
DROP TABLE gt_key_fixed_string;
DROP TABLE gt_serialized;
DROP TABLE gt_nullable_key32;
DROP TABLE gt_nullable_key_string;
DROP TABLE gt_low_cardinality_key64;
DROP TABLE gt_low_cardinality_key_string;
