SELECT CAST(90000::UInt64, 'Time64');
SELECT CAST(90001::UInt64, 'Time64');
SELECT CAST(86400::UInt64, 'Time64');
SELECT CAST(86401::UInt64, 'Time64');

SELECT CAST(3600 * 25, 'Time64');
SELECT CAST((3600 * 25) + 1, 'Time64');

SELECT CAST(86399::UInt64, 'Time64');
SELECT CAST(86400::UInt64, 'Time64');
SELECT CAST(360000::UInt64, 'Time64');

SELECT CAST(90001::UInt32, 'Time64') = CAST(90001::UInt64, 'Time64');

SELECT CAST((3600 * 25) + 1, 'Time');

SELECT CAST(18446744073709551615::UInt64, 'Time64') SETTINGS date_time_overflow_behavior='saturate';
SELECT CAST(9223372036854775808::UInt64, 'Time64') SETTINGS date_time_overflow_behavior='saturate';

SELECT CAST(3600000::UInt64, 'Time64') SETTINGS date_time_overflow_behavior='throw'; -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT CAST(3600000::UInt32, 'Time64') SETTINGS date_time_overflow_behavior='throw'; -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT CAST(9999999::Int64, 'Time64') SETTINGS date_time_overflow_behavior='throw'; -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT CAST(9999999.0::Float64, 'Time64') SETTINGS date_time_overflow_behavior='throw'; -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT CAST(-9999999::Int64, 'Time64') SETTINGS date_time_overflow_behavior='throw'; -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }

SELECT CAST(99999999999::UInt64, 'DateTime64') SETTINGS date_time_overflow_behavior='throw'; -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT CAST(99999999999::Int64, 'DateTime64') SETTINGS date_time_overflow_behavior='throw'; -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT CAST(99999999999.0::Float64, 'DateTime64') SETTINGS date_time_overflow_behavior='throw'; -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }

-- Small unsigned types fit entirely within the valid range, so no throw should occur and the result must agree across widths.
SELECT CAST(200::UInt8, 'Time64') = CAST(200::UInt64, 'Time64') SETTINGS date_time_overflow_behavior='throw';
SELECT CAST(60000::UInt16, 'Time64') = CAST(60000::UInt64, 'Time64') SETTINGS date_time_overflow_behavior='throw';
SELECT CAST(1000000::UInt32, 'Time64') = CAST(1000000::UInt64, 'Time64') SETTINGS date_time_overflow_behavior='throw';
SELECT CAST(200::UInt8, 'DateTime64') = CAST(200::UInt64, 'DateTime64') SETTINGS date_time_overflow_behavior='throw';
SELECT CAST(60000::UInt16, 'DateTime64') = CAST(60000::UInt64, 'DateTime64') SETTINGS date_time_overflow_behavior='throw';
SELECT CAST(4294967295::UInt32, 'DateTime64') = CAST(4294967295::UInt64, 'DateTime64') SETTINGS date_time_overflow_behavior='throw';

-- Saturate mode for unsigned types
SELECT CAST(4294967295::UInt32, 'Time64') = CAST(3599999::UInt64, 'Time64') SETTINGS date_time_overflow_behavior='saturate';

-- Saturate clamp: out-of-range values must compare equal to the max.
-- Integer sources clamp to the whole-second maximum (they carry no fraction),
-- while float sources clamp to the maximum representable fractional value at the target scale.
SELECT CAST(9999999::Int64, 'Time64') = CAST(3599999::Int64, 'Time64') SETTINGS date_time_overflow_behavior='saturate';
SELECT CAST(9999999.0::Float64, 'Time64') = CAST(3599999.999::Float64, 'Time64') SETTINGS date_time_overflow_behavior='saturate';
SELECT CAST(9999999::UInt64, 'Time64') = CAST(3599999::UInt64, 'Time64') SETTINGS date_time_overflow_behavior='saturate';

-- Small types (Int16) must not overflow during clamping
SELECT toTime64(-3600::Int16, 0);
SELECT toTime64(3600::Int16, 0);

-- Wide integer sources must also honor `date_time_overflow_behavior`.
SELECT CAST(99999999999::Int128, 'DateTime64') SETTINGS date_time_overflow_behavior='throw'; -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT CAST(99999999999::Int256, 'DateTime64') SETTINGS date_time_overflow_behavior='throw'; -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT CAST(99999999999::UInt128, 'DateTime64') SETTINGS date_time_overflow_behavior='throw'; -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT CAST(99999999999::UInt256, 'DateTime64') SETTINGS date_time_overflow_behavior='throw'; -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }

SELECT CAST(9999999::Int128, 'Time64') SETTINGS date_time_overflow_behavior='throw'; -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT CAST(9999999::Int256, 'Time64') SETTINGS date_time_overflow_behavior='throw'; -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT CAST(9999999::UInt128, 'Time64') SETTINGS date_time_overflow_behavior='throw'; -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT CAST(9999999::UInt256, 'Time64') SETTINGS date_time_overflow_behavior='throw'; -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT CAST(-9999999::Int128, 'Time64') SETTINGS date_time_overflow_behavior='throw'; -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT CAST(-9999999::Int256, 'Time64') SETTINGS date_time_overflow_behavior='throw'; -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }

-- Saturate clamp also works for wide ints, matching the narrow counterpart at the boundary.
SELECT CAST(9999999::Int128, 'Time64') = CAST(3599999::Int64, 'Time64') SETTINGS date_time_overflow_behavior='saturate';
SELECT CAST(9999999::UInt128, 'Time64') = CAST(3599999::UInt64, 'Time64') SETTINGS date_time_overflow_behavior='saturate';
SELECT CAST(9999999::Int256, 'Time64') = CAST(3599999::Int64, 'Time64') SETTINGS date_time_overflow_behavior='saturate';
SELECT CAST(9999999::UInt256, 'Time64') = CAST(3599999::UInt64, 'Time64') SETTINGS date_time_overflow_behavior='saturate';
SELECT CAST(-9999999::Int128, 'Time64') = CAST(-3599999::Int64, 'Time64') SETTINGS date_time_overflow_behavior='saturate';

-- In-range wide-int values agree with the narrow counterpart.
SELECT CAST(60000::Int128, 'Time64') = CAST(60000::Int64, 'Time64') SETTINGS date_time_overflow_behavior='throw';
SELECT CAST(60000::UInt128, 'Time64') = CAST(60000::UInt64, 'Time64') SETTINGS date_time_overflow_behavior='throw';
SELECT CAST(1234567890::Int128, 'DateTime64') = CAST(1234567890::Int64, 'DateTime64') SETTINGS date_time_overflow_behavior='throw';
SELECT CAST(1234567890::UInt256, 'DateTime64') = CAST(1234567890::UInt64, 'DateTime64') SETTINGS date_time_overflow_behavior='throw';

-- Fractional values inside the boundary second are valid and must not be rejected (throw) or clamped to the whole second.
SELECT CAST(10413791999.1::Float64, 'DateTime64(1, \'UTC\')') SETTINGS date_time_overflow_behavior='throw';
SELECT CAST(10413791999.1::Float64, 'DateTime64(1, \'UTC\')') SETTINGS date_time_overflow_behavior='saturate';
SELECT CAST(3599999.1::Float64, 'Time64(1)') SETTINGS date_time_overflow_behavior='throw';
SELECT CAST(3599999.1::Float64, 'Time64(1)') SETTINGS date_time_overflow_behavior='saturate';
SELECT CAST(-3599999.1::Float64, 'Time64(1)') SETTINGS date_time_overflow_behavior='throw';

-- Values beyond the scale-aware boundary still overflow: throw raises, saturate clamps to the max representable fraction.
SELECT CAST(10413792000.5::Float64, 'DateTime64(1, \'UTC\')') SETTINGS date_time_overflow_behavior='throw'; -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT CAST(10413792000.5::Float64, 'DateTime64(1, \'UTC\')') SETTINGS date_time_overflow_behavior='saturate';
SELECT CAST(3600000.5::Float64, 'Time64(1)') SETTINGS date_time_overflow_behavior='throw'; -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT CAST(3600000.5::Float64, 'Time64(1)') SETTINGS date_time_overflow_behavior='saturate';
