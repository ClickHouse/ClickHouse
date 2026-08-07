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
-- Narrow signed sources (Int8 / Int16 / Int32) went through a separate transform that stored the raw
-- value and ignored the setting, so an out-of-range Int32 stayed verbatim while the 64-bit path threw.
SELECT CAST(4000000::Int32, 'Time'); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT CAST(-4000000::Int32, 'Time'); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
-- UInt32 and the wide integer types (Int128 / UInt128 / Int256 / UInt256 / BFloat16) used to miss every
-- branch of the DateTime/Time dispatch and fall through to convertNumericGeneral, which ignores the
-- setting and truncates. UInt32 4000000 fits DateTime (max UInt32) but overflows Time (max 3599999).
SELECT CAST(4000000::UInt32, 'Time'); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT CAST(340282366920938463463374607431768211455::UInt128, 'DateTime'); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT CAST(340282366920938463463374607431768211455::UInt128, 'Time'); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT CAST(99999999999999999999999999::Int256, 'DateTime'); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT CAST(99999999999999999999999999::Int256, 'Time'); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT CAST(-99999999999999999999999999::Int256, 'DateTime'); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
-- UInt256, Int128 and BFloat16 each have their own alternative in the DateTime/Time dispatch; keep one
-- row per alternative per target so deleting any of them reddens this test rather than passing silently.
SELECT CAST(99999999999999999999999999::UInt256, 'DateTime'); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT CAST(99999999999999999999999999::UInt256, 'Time'); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT CAST(99999999999999999999999999::Int128, 'DateTime'); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT CAST(99999999999999999999999999::Int128, 'Time'); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT CAST(-99999999999999999999999999::Int128, 'Time'); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT CAST(CAST(1e30, 'BFloat16'), 'DateTime'); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT CAST(CAST(1e30, 'BFloat16'), 'Time'); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }

SELECT '-- throw: huge finite floats must raise a clean error, not narrow to garbage';
-- Formatting the rejected value with static_cast<Int64>(from) was undefined behavior for these
-- inputs; the throw path widens floats to double instead.
SELECT CAST(1e300::Float64, 'Date'); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT CAST(3e38::Float32, 'Date'); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT CAST(1e300::Float64, 'Date32'); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT CAST(1e300::Float64, 'DateTime'); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT CAST(1e300::Float64, 'Time'); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }

SELECT '-- non-finite floats are rejected as unconvertible, whatever the overflow mode is';
-- Inf and NaN are not out-of-range values but values the target cannot represent at all, so every
-- mode raises CANNOT_CONVERT_TYPE rather than saturating. Asserted again under saturate/ignore below.
SELECT CAST(inf::Float64, 'Date'); -- { serverError CANNOT_CONVERT_TYPE }
SELECT CAST((-inf)::Float64, 'Date'); -- { serverError CANNOT_CONVERT_TYPE }
SELECT CAST(nan::Float64, 'Date'); -- { serverError CANNOT_CONVERT_TYPE }
SELECT CAST(inf::Float64, 'Date32'); -- { serverError CANNOT_CONVERT_TYPE }
SELECT CAST(nan::Float64, 'Date32'); -- { serverError CANNOT_CONVERT_TYPE }
SELECT CAST(inf::Float64, 'DateTime'); -- { serverError CANNOT_CONVERT_TYPE }
SELECT CAST(nan::Float64, 'DateTime'); -- { serverError CANNOT_CONVERT_TYPE }
SELECT CAST(inf::Float64, 'Time'); -- { serverError CANNOT_CONVERT_TYPE }
SELECT CAST(nan::Float64, 'Time'); -- { serverError CANNOT_CONVERT_TYPE }

SELECT '-- throw: in-range numeric casts must still succeed';
SELECT CAST(1700000000::Int64, 'DateTime');
SELECT CAST(20000::Int64, 'Date');
SELECT CAST(20000::Int64, 'Date32');

SELECT '-- throw: accurateCastOrNull must never raise; only accurateCast does';
-- accurateCastOrNull's contract is a NULL (or the converted value), never an exception, so the numeric
-- temporal transforms are instantiated in ignore mode on that path: rejection is the accurate precheck's
-- job. Enabling the previously dead Throw specializations made a Throw transform reachable there and it
-- raised from inside the vectorised loop. The two carriers the current precheck's single generic-DateTime
-- window misses are a Time target (4000000 is inside 0xFFFFFFFF but above MAX_TIME_TIMESTAMP) and a Date32
-- target (absent from the precheck's type list). PR #110459 corrected those windows, so both now return
-- \N. Either way they must not raise.
SELECT accurateCastOrNull(4000000, 'Time');
SELECT accurateCastOrNull(999999999999, 'Date32');
-- Control: the generic window already rejects this one, so it must keep returning \N.
SELECT accurateCastOrNull(99999999999, 'DateTime');
-- Controls: in-range values must still convert, so the rows above cannot pass by rejecting everything.
SELECT accurateCastOrNull(100, 'Time'), accurateCastOrNull(100, 'DateTime');
-- The throwing sibling must still reject the same values; after #110459 the accurate precheck rejects
-- them itself, so the error is the accurate-cast CANNOT_CONVERT_TYPE rather than the transform's.
SELECT accurateCast(4000000, 'Time'); -- { serverError CANNOT_CONVERT_TYPE }
SELECT accurateCast(999999999999, 'Date32'); -- { serverError CANNOT_CONVERT_TYPE }

SELECT '-- throw: the values() table function does not see the session setting (known boundary)';
-- values() constructs its coercion with a default FormatSettings (TableFunctionValues.cpp), so the session
-- date_time_overflow_behavior never reaches it and it keeps clamping in every mode, while CAST of the same
-- literal now raises. Aligning them would make values() start raising, so it is left to a follow-up; this
-- pair pins the current divergence so it cannot change unnoticed.
SELECT toInt32(c) FROM values('c Time', 9999999);
SELECT CAST(9999999 AS Time); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }

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
-- Narrow signed Int32 -> Time must clamp to the max/min stored integer, not keep the raw out-of-range
-- value. Assert on toInt32 (the printed text 999:59:59 hides the stored 4000000).
SELECT toInt32(CAST(4000000::Int32, 'Time'));
SELECT toInt32(CAST(-4000000::Int32, 'Time'));
-- UInt32 and wide integer types (UInt128 / UInt256 / Int256) must clamp to the boundary too, not truncate
-- on the generic path. Assert the stored integer via toInt32 where the printed text could hide a wrap.
SELECT toInt32(CAST(4000000::UInt32, 'Time'));
SELECT toInt32(CAST(340282366920938463463374607431768211455::UInt128, 'Time'));
SELECT toInt32(CAST(99999999999999999999999999::Int256, 'Time'));
SELECT toInt32(CAST(-99999999999999999999999999::Int256, 'Time'));
SELECT CAST(340282366920938463463374607431768211455::UInt128, 'DateTime');
SELECT CAST(99999999999999999999999999::Int256, 'DateTime');
SELECT CAST(-99999999999999999999999999::Int256, 'DateTime');
SELECT CAST(1e300::Float64, 'Time');
SELECT CAST(1e300::Float64, 'DateTime');
SELECT CAST(3e38::Float32, 'DateTime');
-- Non-finite floats are unconvertible in every mode, so saturate does not clamp them either.
SELECT CAST(inf::Float64, 'DateTime'); -- { serverError CANNOT_CONVERT_TYPE }
SELECT CAST((-inf)::Float64, 'Time'); -- { serverError CANNOT_CONVERT_TYPE }
SELECT CAST(nan::Float64, 'DateTime'); -- { serverError CANNOT_CONVERT_TYPE }

SELECT '-- saturate: UInt256 / Int128 / BFloat16 -> DateTime and Time clamp on their own dispatch alternatives';
-- Keyed on toInt32 for Time so the printed 999:59:59 cannot hide a wrap.
SELECT CAST(99999999999999999999999999::UInt256, 'DateTime');
SELECT toInt32(CAST(99999999999999999999999999::UInt256, 'Time'));
SELECT CAST(99999999999999999999999999::Int128, 'DateTime');
SELECT toInt32(CAST(99999999999999999999999999::Int128, 'Time'));
SELECT CAST(-99999999999999999999999999::Int128, 'DateTime');
SELECT toInt32(CAST(-99999999999999999999999999::Int128, 'Time'));
SELECT CAST(CAST(1e30, 'BFloat16'), 'DateTime');
SELECT toInt32(CAST(CAST(1e30, 'BFloat16'), 'Time'));
SELECT toInt32(CAST(CAST(-1e30, 'BFloat16'), 'Time'));

SELECT '-- saturate: NaN is unconvertible for every target, not silently narrowed';
-- The Float* -> Date branch skipped the day-num/timestamp split for NaN (both from<0 and
-- from>DATE_LUT_MAX_DAY_NUM are false), then reached static_cast<UInt16>(from) = undefined behavior.
-- It is now rejected before that cast, consistently with the sibling Date32 / DateTime / Time paths.
SELECT CAST(nan::Float64, 'Date'); -- { serverError CANNOT_CONVERT_TYPE }
SELECT CAST(nan::Float32, 'Date'); -- { serverError CANNOT_CONVERT_TYPE }
SELECT CAST(nan::Float64, 'Date32'); -- { serverError CANNOT_CONVERT_TYPE }
SELECT CAST(nan::Float64, 'Time'); -- { serverError CANNOT_CONVERT_TYPE }

SELECT '-- ignore (default): out-of-range numeric casts keep the legacy behavior';
SET date_time_overflow_behavior = 'ignore';
SELECT CAST(99999999999::Int64, 'DateTime');
SELECT CAST(99999999999::UInt64, 'DateTime');
SELECT CAST(99999999999::Int64, 'Date');
SELECT CAST(999999999999::Int64, 'Date32');
SELECT '-- ignore: sources above INT64_MAX / float extremes must also clamp, not wrap negative';
SELECT CAST(18446744073709551615::UInt64, 'Time');
SELECT CAST(9223372036854775813::UInt64, 'DateTime');
SELECT '-- ignore: narrow signed Int32 -> Time must clamp too, not store the raw out-of-range value';
SELECT toInt32(CAST(4000000::Int32, 'Time'));
SELECT toInt32(CAST(-4000000::Int32, 'Time'));
SELECT '-- ignore: UInt32 and wide integer types -> DateTime/Time must clamp too, not truncate on the generic path';
SELECT toInt32(CAST(4000000::UInt32, 'Time'));
SELECT toInt32(CAST(340282366920938463463374607431768211455::UInt128, 'Time'));
SELECT CAST(340282366920938463463374607431768211455::UInt128, 'DateTime');
SELECT CAST(99999999999999999999999999::Int256, 'DateTime');
SELECT '-- ignore: NaN is unconvertible here too, not fall through to a narrowing cast';
SELECT CAST(nan::Float64, 'Date'); -- { serverError CANNOT_CONVERT_TYPE }
SELECT CAST(nan::Float64, 'Date32'); -- { serverError CANNOT_CONVERT_TYPE }
SELECT CAST(nan::Float64, 'DateTime'); -- { serverError CANNOT_CONVERT_TYPE }
SELECT CAST(nan::Float64, 'Time'); -- { serverError CANNOT_CONVERT_TYPE }
