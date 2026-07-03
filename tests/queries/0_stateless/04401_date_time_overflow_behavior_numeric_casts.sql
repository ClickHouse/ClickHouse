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

SELECT '-- throw: float extremes (huge / Inf / NaN) must raise a clean error, not narrow to garbage';
-- Formatting the rejected value with static_cast<Int64>(from) was undefined behavior for these
-- inputs; the throw path must widen floats to double instead. NaN must raise here too (it passes
-- every range comparison, so without an explicit guard it would silently fall through to garbage).
SELECT CAST(1e300::Float64, 'Date'); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT CAST(3e38::Float32, 'Date'); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT CAST(inf::Float64, 'Date'); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT CAST((-inf)::Float64, 'Date'); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT CAST(nan::Float64, 'Date'); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT CAST(1e300::Float64, 'Date32'); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT CAST(inf::Float64, 'Date32'); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT CAST(nan::Float64, 'Date32'); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT CAST(1e300::Float64, 'DateTime'); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT CAST(inf::Float64, 'DateTime'); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT CAST(nan::Float64, 'DateTime'); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT CAST(1e300::Float64, 'Time'); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT CAST(inf::Float64, 'Time'); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT CAST(nan::Float64, 'Time'); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }

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

SELECT '-- saturate: sources above INT64_MAX / float extremes must clamp to the max, not wrap negative';
-- The clamp used to narrow the source to time_t BEFORE std::min, so a UInt64 above INT64_MAX wrapped
-- to a small/negative time_t (e.g. UInt64::max -> -1) and a huge/Inf float narrowed via undefined
-- behavior, producing 1970 / -00:00:01 instead of the saturated maximum. Clamp in the source domain first.
SELECT CAST(18446744073709551615::UInt64, 'Time');
SELECT CAST(9223372036854775813::UInt64, 'Time');
SELECT CAST(18446744073709551615::UInt64, 'DateTime');
SELECT CAST(9223372036854775813::UInt64, 'DateTime');
SELECT CAST(18446744073709551615::UInt64, 'Date');
SELECT CAST(18446744073709551615::UInt64, 'Date32');
SELECT CAST(340282366920938463463374607431768211455::UInt128, 'Date32');
SELECT CAST(1e300::Float64, 'Time');
SELECT CAST(1e300::Float64, 'DateTime');
SELECT CAST(3e38::Float32, 'DateTime');
SELECT CAST(inf::Float64, 'DateTime');
SELECT CAST((-inf)::Float64, 'Time');
SELECT CAST(nan::Float64, 'DateTime');

SELECT '-- saturate: NaN must clamp to the minimum for every target, not fall through to a narrowing cast';
-- The Float* -> Date branch skipped the day-num/timestamp split for NaN (both from<0 and
-- from>DATE_LUT_MAX_DAY_NUM are false), then reached static_cast<UInt16>(from) = undefined behavior.
-- NaN must clamp consistently with the sibling Date32 / DateTime / Time paths.
SELECT CAST(nan::Float64, 'Date');
SELECT CAST(nan::Float32, 'Date');
SELECT CAST(nan::Float64, 'Date32');
SELECT CAST(nan::Float64, 'Time');

SELECT '-- ignore (default): out-of-range numeric casts keep the legacy behavior';
SET date_time_overflow_behavior = 'ignore';
SELECT CAST(99999999999::Int64, 'DateTime');
SELECT CAST(99999999999::UInt64, 'DateTime');
SELECT CAST(99999999999::Int64, 'Date');
SELECT CAST(999999999999::Int64, 'Date32');
SELECT '-- ignore: sources above INT64_MAX / float extremes must also clamp, not wrap negative';
SELECT CAST(18446744073709551615::UInt64, 'Time');
SELECT CAST(9223372036854775813::UInt64, 'DateTime');
SELECT '-- ignore: NaN -> Date must clamp to the minimum, not fall through to a narrowing cast';
SELECT CAST(nan::Float64, 'Date');
SELECT CAST(nan::Float64, 'Date32');
SELECT CAST(nan::Float64, 'DateTime');
SELECT CAST(nan::Float64, 'Time');
