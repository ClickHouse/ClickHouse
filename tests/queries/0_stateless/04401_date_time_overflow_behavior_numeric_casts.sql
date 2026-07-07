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
SELECT '-- ignore: narrow signed Int32 -> Time must clamp too, not store the raw out-of-range value';
SELECT toInt32(CAST(4000000::Int32, 'Time'));
SELECT toInt32(CAST(-4000000::Int32, 'Time'));
SELECT '-- ignore: UInt32 and wide integer types -> DateTime/Time must clamp too, not truncate on the generic path';
SELECT toInt32(CAST(4000000::UInt32, 'Time'));
SELECT toInt32(CAST(340282366920938463463374607431768211455::UInt128, 'Time'));
SELECT CAST(340282366920938463463374607431768211455::UInt128, 'DateTime');
SELECT CAST(99999999999999999999999999::Int256, 'DateTime');
SELECT '-- ignore: NaN -> Date must clamp to the minimum, not fall through to a narrowing cast';
SELECT CAST(nan::Float64, 'Date');
SELECT CAST(nan::Float64, 'Date32');
SELECT CAST(nan::Float64, 'DateTime');
SELECT CAST(nan::Float64, 'Time');

-- INSERT ... VALUES with a numeric expression is coerced through convertFieldToType, which
-- previously stored the raw out-of-range value regardless of the setting. It now applies the
-- same rule as toTime / toDateTime / CAST. (Plain integer literals in VALUES go through the
-- text serializer instead, which is a separate pre-existing path shared by all input formats.)
DROP TABLE IF EXISTS t_vals_time;
DROP TABLE IF EXISTS t_vals_datetime;

SELECT '-- throw: out-of-range numeric VALUES expression must raise';
-- The overflow of a VALUES expression may surface as a client or a server error depending
-- on async_insert (the client parses VALUES data), so the error hint below accepts either.
SET date_time_overflow_behavior = 'throw';
CREATE TABLE t_vals_time (x Time) ENGINE = Memory;
CREATE TABLE t_vals_datetime (x DateTime) ENGINE = Memory;
INSERT INTO t_vals_time VALUES (9999999 + 0); -- { error VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
INSERT INTO t_vals_time VALUES (-9999999 + 0); -- { error VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
INSERT INTO t_vals_datetime VALUES (99999999999 + 0); -- { error VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
INSERT INTO t_vals_datetime VALUES (-1 + 0); -- { error VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }

SELECT '-- saturate: out-of-range numeric VALUES expression must clamp to the boundary';
SET date_time_overflow_behavior = 'saturate';
INSERT INTO t_vals_time VALUES (9999999 + 0), (-9999999 + 0);
INSERT INTO t_vals_datetime VALUES (99999999999 + 0), (-1 + 0);
SELECT toInt32(x) FROM t_vals_time ORDER BY x;
SELECT toInt64(x) FROM t_vals_datetime ORDER BY x;

SELECT '-- ignore: out-of-range numeric VALUES expression must clamp too, not store the raw value';
TRUNCATE TABLE t_vals_time;
TRUNCATE TABLE t_vals_datetime;
SET date_time_overflow_behavior = 'ignore';
INSERT INTO t_vals_time VALUES (9999999 + 0), (-9999999 + 0);
INSERT INTO t_vals_datetime VALUES (99999999999 + 0), (-1 + 0);
SELECT toInt32(x) FROM t_vals_time ORDER BY x;
SELECT toInt64(x) FROM t_vals_datetime ORDER BY x;

SELECT '-- in-range numeric VALUES expression is unchanged in all modes';
TRUNCATE TABLE t_vals_time;
INSERT INTO t_vals_time VALUES (3600 + 0), (-3600 + 0);
SELECT toInt32(x) FROM t_vals_time ORDER BY x;

DROP TABLE t_vals_time;
DROP TABLE t_vals_datetime;

-- Date / Date32 VALUES expressions also reach convertFieldToType and used to ignore the setting:
-- Date/Date32 returned Null (defaulting) or narrowed the raw value through the serializer. They must
-- respect date_time_overflow_behavior with the same day-number / unix-timestamp interpretation as CAST.
SELECT '-- throw: out-of-range Date / Date32 VALUES expression must raise';
SET date_time_overflow_behavior = 'throw';
CREATE TABLE t_vals_date (x Date) ENGINE = Memory;
CREATE TABLE t_vals_date32 (x Date32) ENGINE = Memory;
INSERT INTO t_vals_date VALUES (99999999999 + 0); -- { error VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
INSERT INTO t_vals_date VALUES (-1 + 0); -- { error VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
INSERT INTO t_vals_date32 VALUES (999999999999 + 0); -- { error VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
INSERT INTO t_vals_date32 VALUES (-999999999999 + 0); -- { error VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }

SELECT '-- saturate: out-of-range Date / Date32 VALUES expression must clamp to the boundary';
SET date_time_overflow_behavior = 'saturate';
INSERT INTO t_vals_date VALUES (99999999999 + 0);
INSERT INTO t_vals_date32 VALUES (999999999999 + 0), (-999999999999 + 0);
SELECT toString(x) FROM t_vals_date ORDER BY x;
SELECT toString(x) FROM t_vals_date32 ORDER BY x;

SELECT '-- ignore: out-of-range Date / Date32 VALUES expression must clamp too';
TRUNCATE TABLE t_vals_date;
TRUNCATE TABLE t_vals_date32;
SET date_time_overflow_behavior = 'ignore';
INSERT INTO t_vals_date VALUES (99999999999 + 0);
INSERT INTO t_vals_date32 VALUES (999999999999 + 0), (-999999999999 + 0);
SELECT toString(x) FROM t_vals_date ORDER BY x;
SELECT toString(x) FROM t_vals_date32 ORDER BY x;

DROP TABLE t_vals_date;
DROP TABLE t_vals_date32;

-- Non-Int64/UInt64 field types that evaluateConstantExpression can produce (Float64, wide integers)
-- must be coerced through the same overflow-aware path for every target, not fall through to
-- TYPE_MISMATCH or the raw serializer.
SELECT '-- saturate: Float64 and wide-integer VALUES expressions clamp for every target';
SET date_time_overflow_behavior = 'saturate';
CREATE TABLE t_wide (d Date, d32 Date32, dt DateTime, t Time) ENGINE = Memory;
INSERT INTO t_wide VALUES (1e12 + 0.0, 1e12 + 0.0, 1e12 + 0.0, 1e12 + 0.0);
INSERT INTO t_wide VALUES (toUInt128(99999999999) + toUInt128(0), toInt128(999999999999) + toInt128(0), toUInt256(99999999999) + toUInt256(0), toInt256(999999999999) + toInt256(0));
SELECT toString(d), toString(d32), toString(dt), toString(t) FROM t_wide ORDER BY dt;
DROP TABLE t_wide;

-- The VALUES/INSERT coercion path must agree with the numeric CAST path for the same source value,
-- especially across the Date day-number / unix-timestamp boundary (65535 is the last day number,
-- 65536 is reinterpreted as a unix timestamp). The two SELECTs below must print identical values.
SELECT '-- VALUES coercion agrees with CAST across the Date day-number / timestamp boundary (saturate)';
SET date_time_overflow_behavior = 'saturate';
CREATE TABLE t_parity (x Date) ENGINE = Memory;
INSERT INTO t_parity VALUES (65535 + 0), (65536 + 0), (100000 + 0), (5662310399 + 0);
SELECT 'VALUES', toString(x) FROM t_parity ORDER BY x;
SELECT 'CAST  ', toString(CAST(v AS Date)) FROM (SELECT arrayJoin([65535, 65536, 100000, 5662310399]) AS v) ORDER BY CAST(v AS Date);
DROP TABLE t_parity;
