-- Numeric -> DateTime64 / Time64 conversions must honour `date_time_overflow_behavior`.

SET enable_time_time64_type = 1;
SET session_timezone = 'UTC';

SELECT 'throw';
SET date_time_overflow_behavior = 'throw';
SELECT CAST(99999999999999::UInt64, 'DateTime64(3)'); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT CAST(-99999999999999::Int64, 'DateTime64(3)'); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT CAST(1e30::Float64, 'DateTime64(3)'); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT CAST(3600000::UInt64, 'Time64(3)'); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT CAST(-3600000::Int64, 'Time64(3)'); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT CAST(3.6e6::Float64, 'Time64(3)'); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT toDateTime64(materialize(99999999999999::UInt64), 3); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT toTime64(materialize(3.6e6::Float64), 3); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT accurateCast(1e30::Float64, 'DateTime64(3)'); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
-- The same setting reaches the numeric conversions to `Date`, `Date32`, `DateTime` and `Time`.
SELECT CAST(99999999999::UInt64, 'Date'); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT CAST(1e30::Float64, 'Date32'); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT CAST(99999999999::UInt64, 'DateTime'); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT CAST(-5::Int64, 'DateTime'); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT CAST(3600000::UInt64, 'Time'); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
-- The value is representable, so nothing is thrown.
SELECT CAST(1735689600::UInt32, 'DateTime64(3)'), CAST(3599999::UInt64, 'Time64(3)');
-- `accurateCastOrNull` reports an unrepresentable value instead of throwing.
SELECT accurateCastOrNull(1e30::Float64, 'DateTime64(3)');

SELECT 'ignore';
SET date_time_overflow_behavior = 'ignore';
SELECT CAST(99999999999999::UInt64, 'DateTime64(3)'), CAST(-99999999999999::Int64, 'DateTime64(3)'), CAST(1e30::Float64, 'DateTime64(3)');
SELECT CAST(3600000::UInt64, 'Time64(3)'), CAST(-3600000::Int64, 'Time64(3)'), CAST(3.6e6::Float64, 'Time64(3)');
SELECT CAST(99999999999::UInt64, 'Date'), CAST(1e30::Float64, 'Date32'), CAST(99999999999::UInt64, 'DateTime'), CAST(-5::Int64, 'DateTime'), CAST(3600000::UInt64, 'Time');

SELECT 'saturate';
SET date_time_overflow_behavior = 'saturate';
SELECT CAST(99999999999999::UInt64, 'DateTime64(3)'), CAST(-99999999999999::Int64, 'DateTime64(3)'), CAST(1e30::Float64, 'DateTime64(3)');
SELECT CAST(3600000::UInt64, 'Time64(3)'), CAST(-3600000::Int64, 'Time64(3)'), CAST(3.6e6::Float64, 'Time64(3)');
SELECT CAST(99999999999::UInt64, 'Date'), CAST(1e30::Float64, 'Date32'), CAST(99999999999::UInt64, 'DateTime'), CAST(-5::Int64, 'DateTime'), CAST(3600000::UInt64, 'Time');
