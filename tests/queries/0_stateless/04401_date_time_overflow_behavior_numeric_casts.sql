-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/101131
-- date_time_overflow_behavior was silently ignored when casting numeric types to
-- Date / Date32 / DateTime / Time: the transforms were instantiated with the
-- compile-time constant default_date_time_overflow_behavior (Ignore) instead of the
-- runtime setting, making the throw and saturate paths dead code.
-- (The DateTime64 / Time64 numeric casts are handled separately in PR #101512.)

SET session_timezone = 'UTC';
SET allow_experimental_time_time64_type = 1;

SELECT '-- throw: out-of-range numeric casts must raise VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE';
SET date_time_overflow_behavior = 'throw';
SELECT CAST(99999999999::Int64, 'DateTime'); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT CAST(99999999999::UInt64, 'DateTime'); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT CAST(99999999999.0::Float64, 'DateTime'); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT CAST(99999999999.0::Float32, 'DateTime'); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT CAST(-1::Int64, 'DateTime'); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT CAST(99999999999::Int64, 'Date'); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT CAST(99999999999::UInt64, 'Date'); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT CAST(99999999999.0::Float64, 'Date'); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT CAST(-1::Int64, 'Date'); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT CAST(999999999999::Int64, 'Date32'); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT CAST(999999999999.0::Float64, 'Date32'); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT CAST(999999999999::Int64, 'Time'); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT CAST(999999999999::UInt64, 'Time'); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }

SELECT '-- throw: in-range numeric casts must still succeed';
SELECT CAST(1700000000::Int64, 'DateTime');
SELECT CAST(20000::Int64, 'Date');
SELECT CAST(20000::Int64, 'Date32');

SELECT '-- saturate: out-of-range numeric casts must clamp to the boundary';
SET date_time_overflow_behavior = 'saturate';
SELECT CAST(99999999999::Int64, 'DateTime');
SELECT CAST(99999999999::UInt64, 'DateTime');
SELECT CAST(99999999999.0::Float64, 'DateTime');
SELECT CAST(-1::Int64, 'DateTime');
SELECT CAST(99999999999::Int64, 'Date');
SELECT CAST(-1::Int64, 'Date');
SELECT CAST(999999999999::Int64, 'Date32');

SELECT '-- ignore (default): out-of-range numeric casts keep the legacy behavior';
SET date_time_overflow_behavior = 'ignore';
SELECT CAST(99999999999::Int64, 'DateTime');
SELECT CAST(99999999999::UInt64, 'DateTime');
SELECT CAST(99999999999::Int64, 'Date');
SELECT CAST(999999999999::Int64, 'Date32');
