-- `accurateCast` and `accurateCastOrNull` must enforce the accurate-cast contract for numeric sources to
-- `DateTime64` / `Time64`: a value that is not representable in the result type throws or yields NULL,
-- it is never saturated or wrapped, regardless of `date_time_overflow_behavior`.
-- Related: https://github.com/ClickHouse/ClickHouse/issues/100471

SET session_timezone = 'UTC';

SELECT '-- in-range values are unaffected';
SET date_time_overflow_behavior = 'saturate';
SELECT accurateCast(1000000000::UInt64, 'DateTime64(3)'), accurateCastOrNull(1000000000::UInt64, 'DateTime64(3)');
SELECT accurateCast(-3600::Int64, 'DateTime64(3)'), accurateCastOrNull(-3600::Int64, 'DateTime64(3)');
SELECT accurateCast(-3600::Int64, 'Time64(3)'), accurateCastOrNull(-3600::Int64, 'Time64(3)');
SELECT accurateCast(1000.5::Float64, 'DateTime64(3)'), accurateCastOrNull(1000.5::Float64, 'DateTime64(3)');

SELECT '-- the boundary itself is representable';
SELECT accurateCastOrNull(253402300799::Int64, 'DateTime64(3)'), accurateCastOrNull(253402300800::Int64, 'DateTime64(3)');
SELECT accurateCastOrNull(3599999::Int64, 'Time64(0)'), accurateCastOrNull(3600000::Int64, 'Time64(0)');

SELECT '-- out-of-range: accurateCast throws in every overflow mode';
SET date_time_overflow_behavior = 'saturate';
SELECT accurateCast(300000000000::UInt64, 'DateTime64(9)'); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT accurateCast(-300000000000::Int64, 'DateTime64(3)'); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT accurateCast(300000000000::Int64, 'Time64(3)'); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT accurateCast(1e300::Float64, 'DateTime64(3)'); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SET date_time_overflow_behavior = 'ignore';
SELECT accurateCast(300000000000::UInt64, 'DateTime64(9)'); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SET date_time_overflow_behavior = 'throw';
SELECT accurateCast(300000000000::UInt64, 'DateTime64(9)'); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }

SELECT '-- out-of-range: accurateCastOrNull yields NULL in every overflow mode';
SET date_time_overflow_behavior = 'saturate';
SELECT accurateCastOrNull(300000000000::UInt64, 'DateTime64(9)');
SELECT accurateCastOrNull(-300000000000::Int64, 'DateTime64(3)');
SELECT accurateCastOrNull(300000000000::Int64, 'Time64(3)');
SELECT accurateCastOrNull(1e300::Float64, 'DateTime64(3)');
SELECT accurateCastOrNull(nan::Float64, 'DateTime64(3)'), accurateCastOrNull(inf::Float64, 'Time64(3)');
SET date_time_overflow_behavior = 'ignore';
SELECT accurateCastOrNull(300000000000::UInt64, 'DateTime64(9)');
SET date_time_overflow_behavior = 'throw';
SELECT accurateCastOrNull(300000000000::UInt64, 'DateTime64(9)');

SELECT '-- columns';
SET date_time_overflow_behavior = 'saturate';
SELECT accurateCastOrNull(x, 'DateTime64(9)') FROM (SELECT materialize(300000000000::UInt64) AS x);
SELECT accurateCastOrNull(x, 'DateTime64(3)') FROM (SELECT materialize(nan::Float64) AS x);
SELECT accurateCast(x, 'DateTime64(9)') FROM (SELECT materialize(300000000000::UInt64) AS x); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT accurateCast(x, 'DateTime64(3)') FROM (SELECT materialize(1000000000::UInt64) AS x);
