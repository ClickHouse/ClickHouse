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
-- The same setting reaches the numeric conversions to `Date`, `Date32`, `DateTime` and `Time`.
SELECT CAST(99999999999::UInt64, 'Date'); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT CAST(1e30::Float64, 'Date32'); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT CAST(99999999999::UInt64, 'DateTime'); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT CAST(-5::Int64, 'DateTime'); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT CAST(3600000::UInt64, 'Time'); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
-- The value is representable, so nothing is thrown.
SELECT CAST(1735689600::UInt32, 'DateTime64(3)'), CAST(3599999::UInt64, 'Time64(3)');

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

-- The accurate casts keep their contract in every mode: `accurateCast` rejects an unrepresentable value instead of
-- clamping it, `accurateCastOrNull` reports it as NULL and `accurateCastOrDefault` substitutes the default.
SELECT 'accurate';
SELECT accurateCast(1735689600::UInt32, 'DateTime64(3)'), accurateCast(-1.5::Float64, 'DateTime64(3)'), accurateCast(-3599999::Int64, 'Time64(3)'), accurateCast(3599999.5::Float64, 'Time64(3)');
SELECT accurateCastOrNull(1735689600::UInt32, 'DateTime64(3)'), accurateCastOrNull(-1.5::Float64, 'DateTime64(3)'), accurateCastOrNull(-3599999::Int64, 'Time64(3)'), accurateCastOrNull(3599999.5::Float64, 'Time64(3)');
-- The whole-seconds bound of `DateTime64` depends on the scale: 2262-04-11 is the last day of a scale-9 value.
SELECT accurateCast(9223372036::Int64, 'DateTime64(9)'), accurateCastOrNull(9223372037::Int64, 'DateTime64(9)'), accurateCast(9223372037::Int64, 'DateTime64(8)');
-- A floating-point source is checked against the last representable tick, not against the last representable
-- whole second, so the fractional tail of that second is accepted rather than rejected as an overflow.
SELECT accurateCast(9223372036.5::Float64, 'DateTime64(9)'), accurateCastOrNull(9223372037.0::Float64, 'DateTime64(9)'), accurateCastOrNull(-3600000.5::Float64, 'Time64(1)');

SELECT accurateCast(99999999999999::UInt64, 'DateTime64(3)') SETTINGS date_time_overflow_behavior = 'throw'; -- { serverError CANNOT_CONVERT_TYPE }
SELECT accurateCast(99999999999999::UInt64, 'DateTime64(3)') SETTINGS date_time_overflow_behavior = 'ignore'; -- { serverError CANNOT_CONVERT_TYPE }
SELECT accurateCast(99999999999999::UInt64, 'DateTime64(3)') SETTINGS date_time_overflow_behavior = 'saturate'; -- { serverError CANNOT_CONVERT_TYPE }
SELECT accurateCast(-99999999999999::Int64, 'DateTime64(3)') SETTINGS date_time_overflow_behavior = 'ignore'; -- { serverError CANNOT_CONVERT_TYPE }
SELECT accurateCast(1e30::Float64, 'DateTime64(3)') SETTINGS date_time_overflow_behavior = 'throw'; -- { serverError CANNOT_CONVERT_TYPE }
SELECT accurateCast(1e30::Float64, 'DateTime64(3)') SETTINGS date_time_overflow_behavior = 'saturate'; -- { serverError CANNOT_CONVERT_TYPE }
SELECT accurateCast(nan::Float64, 'DateTime64(3)') SETTINGS date_time_overflow_behavior = 'ignore'; -- { serverError CANNOT_CONVERT_TYPE }
SELECT accurateCast(9223372037::Int64, 'DateTime64(9)') SETTINGS date_time_overflow_behavior = 'ignore'; -- { serverError CANNOT_CONVERT_TYPE }
SELECT accurateCast(3600000::UInt64, 'Time64(3)') SETTINGS date_time_overflow_behavior = 'throw'; -- { serverError CANNOT_CONVERT_TYPE }
SELECT accurateCast(3600000::UInt64, 'Time64(3)') SETTINGS date_time_overflow_behavior = 'ignore'; -- { serverError CANNOT_CONVERT_TYPE }
SELECT accurateCast(-3600000::Int64, 'Time64(3)') SETTINGS date_time_overflow_behavior = 'saturate'; -- { serverError CANNOT_CONVERT_TYPE }
SELECT accurateCast(3.6e6::Float64, 'Time64(3)') SETTINGS date_time_overflow_behavior = 'ignore'; -- { serverError CANNOT_CONVERT_TYPE }

SELECT accurateCastOrNull(99999999999999::UInt64, 'DateTime64(3)'), accurateCastOrNull(-99999999999999::Int64, 'DateTime64(3)'), accurateCastOrNull(1e30::Float64, 'DateTime64(3)'), accurateCastOrNull(nan::Float64, 'DateTime64(3)') SETTINGS date_time_overflow_behavior = 'throw';
SELECT accurateCastOrNull(99999999999999::UInt64, 'DateTime64(3)'), accurateCastOrNull(-99999999999999::Int64, 'DateTime64(3)'), accurateCastOrNull(1e30::Float64, 'DateTime64(3)'), accurateCastOrNull(nan::Float64, 'DateTime64(3)') SETTINGS date_time_overflow_behavior = 'ignore';
SELECT accurateCastOrNull(99999999999999::UInt64, 'DateTime64(3)'), accurateCastOrNull(-99999999999999::Int64, 'DateTime64(3)'), accurateCastOrNull(1e30::Float64, 'DateTime64(3)'), accurateCastOrNull(nan::Float64, 'DateTime64(3)') SETTINGS date_time_overflow_behavior = 'saturate';
SELECT accurateCastOrNull(3600000::UInt64, 'Time64(3)'), accurateCastOrNull(-3600000::Int64, 'Time64(3)'), accurateCastOrNull(3.6e6::Float64, 'Time64(3)') SETTINGS date_time_overflow_behavior = 'throw';
SELECT accurateCastOrNull(3600000::UInt64, 'Time64(3)'), accurateCastOrNull(-3600000::Int64, 'Time64(3)'), accurateCastOrNull(3.6e6::Float64, 'Time64(3)') SETTINGS date_time_overflow_behavior = 'ignore';
SELECT accurateCastOrNull(3600000::UInt64, 'Time64(3)'), accurateCastOrNull(-3600000::Int64, 'Time64(3)'), accurateCastOrNull(3.6e6::Float64, 'Time64(3)') SETTINGS date_time_overflow_behavior = 'saturate';

SELECT accurateCastOrDefault(99999999999999::UInt64, 'DateTime64(3)'), accurateCastOrDefault(1e30::Float64, 'DateTime64(3)', toDateTime64('2025-01-01 00:00:00', 3)), accurateCastOrDefault(3600000::UInt64, 'Time64(3)'), accurateCastOrDefault(-3600000::Int64, 'Time64(3)', toTime64('1:00:00', 3)) SETTINGS date_time_overflow_behavior = 'ignore';
SELECT accurateCastOrDefault(99999999999999::UInt64, 'DateTime64(3)'), accurateCastOrDefault(1e30::Float64, 'DateTime64(3)', toDateTime64('2025-01-01 00:00:00', 3)), accurateCastOrDefault(3600000::UInt64, 'Time64(3)'), accurateCastOrDefault(-3600000::Int64, 'Time64(3)', toTime64('1:00:00', 3)) SETTINGS date_time_overflow_behavior = 'saturate';

-- A `Date32` day is not always representable as a high-precision `DateTime64` either, and the accurate casts
-- report that instead of silently clamping to the boundary of the scale.
SELECT accurateCastOrNull(toDate32('2299-12-31'), 'DateTime64(9)'), accurateCastOrNull(toDate32('2262-04-11'), 'DateTime64(9)'), accurateCastOrNull(toDate32('2299-12-31'), 'DateTime64(3)');
SELECT accurateCast(toDate32('2299-12-31'), 'DateTime64(9)'); -- { serverError CANNOT_CONVERT_TYPE }
SELECT accurateCastOrDefault(toDate32('2299-12-31'), 'DateTime64(9)', toDateTime64('2025-01-01 00:00:00', 9));

-- A fraction that the target scale cannot express is truncated, not rejected: `DateTime64` and `Time64` are
-- `Decimal` carriers, and `accurateCast` to a `Decimal` truncates the extra digits the same way.
SELECT accurateCast(1.25::Float64, 'DateTime64(1)'), accurateCastOrNull(1.25::Float64, 'Time64(1)'), accurateCast(1.25::Float64, 'Decimal64(1)');

-- The same holds for a block of values, where only the unrepresentable rows become NULL.
SELECT number, accurateCastOrNull(number * 100000000000::UInt64, 'DateTime64(3)'), accurateCastOrNull(toInt64(number) * 1000000 - 2000000, 'Time64(3)') FROM numbers(5) SETTINGS date_time_overflow_behavior = 'ignore';

-- Widening the scale of a `DateTime64` can leave the representable window of the target as well - a scale-0 value
-- in the year 2299 has no scale-9 representation - and that is reported through `date_time_overflow_behavior` and
-- the accurate casts instead of failing with `DECIMAL_OVERFLOW`.
SELECT CAST(toDateTime64('2299-12-31 00:00:00', 0, 'UTC'), 'DateTime64(9, \'UTC\')') SETTINGS date_time_overflow_behavior = 'saturate';
SELECT CAST(toDateTime64('2299-12-31 00:00:00', 0, 'UTC'), 'DateTime64(9, \'UTC\')') SETTINGS date_time_overflow_behavior = 'ignore';
SELECT CAST(toDateTime64('2299-12-31 00:00:00', 0, 'UTC'), 'DateTime64(9, \'UTC\')') SETTINGS date_time_overflow_behavior = 'throw'; -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT CAST(toDateTime64('1000-01-01 00:00:00', 0, 'UTC'), 'DateTime64(9, \'UTC\')') SETTINGS date_time_overflow_behavior = 'saturate';
SELECT toDateTime64(toDateTime64('2299-12-31 00:00:00', 0, 'UTC'), 9, 'UTC') SETTINGS date_time_overflow_behavior = 'saturate';
SELECT accurateCastOrNull(toDateTime64('2299-12-31 00:00:00', 0, 'UTC'), 'DateTime64(9, \'UTC\')'), accurateCastOrNull(toDateTime64('2200-01-01 00:00:00', 0, 'UTC'), 'DateTime64(3, \'UTC\')');
SELECT accurateCast(toDateTime64('2299-12-31 00:00:00', 0, 'UTC'), 'DateTime64(9, \'UTC\')'); -- { serverError CANNOT_CONVERT_TYPE }
SELECT accurateCastOrDefault(toDateTime64('2299-12-31 00:00:00', 0, 'UTC'), 'DateTime64(9, \'UTC\')', toDateTime64('2025-01-01 00:00:00', 9, 'UTC'));
-- The boundaries of the scale-9 window round-trip, and narrowing the scale never overflows.
SELECT CAST(toDateTime64('2262-04-11 00:00:00', 0, 'UTC'), 'DateTime64(9, \'UTC\')'), CAST(toDateTime64('2262-04-11 00:00:00', 9, 'UTC'), 'DateTime64(0, \'UTC\')');
