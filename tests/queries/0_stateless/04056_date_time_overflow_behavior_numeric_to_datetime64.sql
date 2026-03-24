-- Test that date_time_overflow_behavior = 'throw' is respected for numeric-to-DateTime64/Time64 casts,
-- both for constant expressions (constant folding) and non-constant (runtime) paths.
-- https://github.com/ClickHouse/ClickHouse/issues/100471

SET session_timezone = 'UTC';
SET allow_experimental_time_time64_type = 1;

SELECT '--- throw: constant signed int -> DateTime64 ---';
SELECT CAST(-999999999999::Int64, 'DateTime64(3)') SETTINGS date_time_overflow_behavior = 'throw'; -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT CAST(999999999999::Int64, 'DateTime64(3)') SETTINGS date_time_overflow_behavior = 'throw'; -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }

SELECT '--- throw: constant unsigned int -> DateTime64 ---';
SELECT CAST(99999999999::UInt64, 'DateTime64(3)') SETTINGS date_time_overflow_behavior = 'throw'; -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }

SELECT '--- throw: constant float -> DateTime64 ---';
SELECT CAST(99999999999.0::Float64, 'DateTime64(3)') SETTINGS date_time_overflow_behavior = 'throw'; -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT CAST(-99999999999.0::Float64, 'DateTime64(3)') SETTINGS date_time_overflow_behavior = 'throw'; -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }

SELECT '--- throw: constant signed int -> Time64 ---';
SELECT CAST(3600000::Int64, 'Time64') SETTINGS date_time_overflow_behavior = 'throw'; -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT CAST(-3600000::Int64, 'Time64') SETTINGS date_time_overflow_behavior = 'throw'; -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }

SELECT '--- throw: constant unsigned int -> Time64 ---';
SELECT CAST(3600000::UInt64, 'Time64') SETTINGS date_time_overflow_behavior = 'throw'; -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }

SELECT '--- throw: constant float -> Time64 ---';
SELECT CAST(99999999999.0::Float64, 'Time64') SETTINGS date_time_overflow_behavior = 'throw'; -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }

SELECT '--- throw: non-constant paths via table ---';
DROP TABLE IF EXISTS overflow_test;
CREATE TABLE overflow_test
(
    i64 Int64,
    u64 UInt64,
    f64 Float64
) ENGINE = Memory;

INSERT INTO overflow_test VALUES (-999999999999, 99999999999, 99999999999.0);
INSERT INTO overflow_test VALUES (999999999999, 3600000, -99999999999.0);

SELECT toDateTime64(i64, 3) FROM overflow_test SETTINGS date_time_overflow_behavior = 'throw'; -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT toDateTime64(u64, 3) FROM overflow_test LIMIT 1 SETTINGS date_time_overflow_behavior = 'throw'; -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT toDateTime64(f64, 3) FROM overflow_test LIMIT 1 SETTINGS date_time_overflow_behavior = 'throw'; -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT CAST(u64, 'DateTime64(3)') FROM overflow_test LIMIT 1 SETTINGS date_time_overflow_behavior = 'throw'; -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT toTime64(u64, 0) FROM overflow_test WHERE u64 = 3600000 SETTINGS date_time_overflow_behavior = 'throw'; -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }

SELECT '--- saturate: values should clamp, not throw ---';
SELECT CAST(99999999999::UInt64, 'DateTime64(3)') SETTINGS date_time_overflow_behavior = 'saturate';
SELECT CAST(-999999999999::Int64, 'DateTime64(3)') SETTINGS date_time_overflow_behavior = 'saturate';
SELECT CAST(99999999999.0::Float64, 'DateTime64(3)') SETTINGS date_time_overflow_behavior = 'saturate';

SELECT '--- ignore: values should clamp silently ---';
SELECT CAST(99999999999::UInt64, 'DateTime64(3)') SETTINGS date_time_overflow_behavior = 'ignore';
SELECT CAST(-999999999999::Int64, 'DateTime64(3)') SETTINGS date_time_overflow_behavior = 'ignore';

DROP TABLE overflow_test;
