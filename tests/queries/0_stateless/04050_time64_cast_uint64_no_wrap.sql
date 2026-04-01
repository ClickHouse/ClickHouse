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
SELECT CAST(9999999::Int64, 'Time64') SETTINGS date_time_overflow_behavior='throw'; -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT CAST(9999999.0::Float64, 'Time64') SETTINGS date_time_overflow_behavior='throw'; -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT CAST(-9999999::Int64, 'Time64') SETTINGS date_time_overflow_behavior='throw'; -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }

SELECT CAST(99999999999::UInt64, 'DateTime64') SETTINGS date_time_overflow_behavior='throw'; -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT CAST(99999999999::Int64, 'DateTime64') SETTINGS date_time_overflow_behavior='throw'; -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT CAST(99999999999.0::Float64, 'DateTime64') SETTINGS date_time_overflow_behavior='throw'; -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }

-- Saturate clamp: out-of-range values must compare equal to the max
SELECT CAST(9999999::Int64, 'Time64') = CAST(3599999::Int64, 'Time64') SETTINGS date_time_overflow_behavior='saturate';
SELECT CAST(9999999.0::Float64, 'Time64') = CAST(3599999.0::Float64, 'Time64') SETTINGS date_time_overflow_behavior='saturate';
SELECT CAST(9999999::UInt64, 'Time64') = CAST(3599999::UInt64, 'Time64') SETTINGS date_time_overflow_behavior='saturate';

-- Small types (Int16) must not overflow during clamping
SELECT toTime64(-3600::Int16, 0);
SELECT toTime64(3600::Int16, 0);
