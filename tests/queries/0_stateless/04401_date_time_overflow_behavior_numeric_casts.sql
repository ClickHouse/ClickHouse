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

-- The overflow-aware coercion above applies only in the explicit saturate/throw modes. In the default
-- ignore mode the Date/Date32/Time coercion still rejects an out-of-storage-range value (Null ->
-- ARGUMENT_OUT_OF_BOUND in convertFieldToTypeOrThrow), so a DROP/OPTIMIZE PARTITION with a bogus numeric
-- literal is rejected instead of being silently reinterpreted and dropping the wrong partition.
SELECT '-- ignore (default): out-of-range numeric partition literal must be rejected, not silently clamped';
SET date_time_overflow_behavior = 'ignore';
DROP TABLE IF EXISTS t_part_date;
CREATE TABLE t_part_date (d Date, x UInt32) ENGINE = MergeTree PARTITION BY d ORDER BY x;
INSERT INTO t_part_date VALUES ('2012-04-05', 1);
ALTER TABLE t_part_date DROP PARTITION 20200523; -- { serverError ARGUMENT_OUT_OF_BOUND }
ALTER TABLE t_part_date DROP PARTITION tuple(toInt64(20200523)); -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT count() FROM t_part_date;
DROP TABLE t_part_date;

DROP TABLE IF EXISTS t_part_time;
CREATE TABLE t_part_time (id UInt64, p Time) ENGINE = MergeTree PARTITION BY p ORDER BY id;
INSERT INTO t_part_time SELECT number, 0 FROM numbers(3);
OPTIMIZE TABLE t_part_time PARTITION -2147483649 FINAL; -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT count() FROM t_part_time;
DROP TABLE t_part_time;

-- Exact-conversion contract: temporal types are integer-backed, so an inexact float literal like 0.1
-- must be rejected for exact-target callers (strict IN, and convert_inexact_floats=false: the
-- DROP/OPTIMIZE PARTITION path via convertFieldToTypeOrThrow), instead of being truncated to 0. Only
-- value-materialization paths (VALUES/INSERT) opt into the lossy CAST-like truncation.
SELECT '-- exact contract: inexact float partition literal must be rejected for every temporal target (ignore)';
SET date_time_overflow_behavior = 'ignore';
DROP TABLE IF EXISTS t_part_ex_dt;
CREATE TABLE t_part_ex_dt (d DateTime, x UInt32) ENGINE = MergeTree PARTITION BY d ORDER BY x;
INSERT INTO t_part_ex_dt VALUES (toDateTime(0), 1), (toDateTime(100), 2);
ALTER TABLE t_part_ex_dt DROP PARTITION 0.1; -- { serverError ARGUMENT_OUT_OF_BOUND }
ALTER TABLE t_part_ex_dt DROP PARTITION 100.0; -- integral float is exactly representable, drops the 100s partition
SELECT count() FROM t_part_ex_dt;
DROP TABLE t_part_ex_dt;

DROP TABLE IF EXISTS t_part_ex_d;
CREATE TABLE t_part_ex_d (d Date, x UInt32) ENGINE = MergeTree PARTITION BY d ORDER BY x;
INSERT INTO t_part_ex_d VALUES (toDate(0), 1);
ALTER TABLE t_part_ex_d DROP PARTITION 0.1; -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT count() FROM t_part_ex_d;
DROP TABLE t_part_ex_d;

-- The same exact-target rejection must also hold in saturate/throw modes (the overflow-aware coerce path).
SELECT '-- exact contract: inexact float partition literal rejected in saturate mode too';
SET date_time_overflow_behavior = 'saturate';
DROP TABLE IF EXISTS t_part_ex_sat;
CREATE TABLE t_part_ex_sat (d DateTime, x UInt32) ENGINE = MergeTree PARTITION BY d ORDER BY x;
INSERT INTO t_part_ex_sat VALUES (toDateTime(0), 1);
ALTER TABLE t_part_ex_sat DROP PARTITION 0.1; -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT count() FROM t_part_ex_sat;
DROP TABLE t_part_ex_sat;

-- Strict IN: an inexact float must not match a temporal value (like =), while an exactly representable
-- integral float still matches. Checked in ignore and saturate modes for every temporal target.
SELECT '-- exact contract: strict IN with inexact float does not match; integral float does (ignore)';
SET date_time_overflow_behavior = 'ignore';
SELECT toDateTime(0) IN (0.1), toDate(0) IN (0.1), toDate32(0) IN (0.1), toTime(0) IN (0.1);
SELECT toDateTime(100) IN (100.0), toDateTime(100) IN (100.5);
SELECT '-- exact contract: strict IN with inexact float does not match (saturate)';
SET date_time_overflow_behavior = 'saturate';
SELECT toDateTime(0) IN (0.1), toDate(0) IN (0.1), toDate32(0) IN (0.1), toTime(0) IN (0.1);

-- Value-materialization (VALUES) still truncates an inexact float like CAST (convert_inexact_floats=true).
SELECT '-- value materialization: VALUES still truncates inexact float like CAST';
SET date_time_overflow_behavior = 'ignore';
DROP TABLE IF EXISTS t_val_ex;
CREATE TABLE t_val_ex (d DateTime) ENGINE = Memory;
INSERT INTO t_val_ex VALUES (0.1);
SELECT toString(d) FROM t_val_ex;
SELECT toString(CAST(0.1 AS DateTime));
DROP TABLE t_val_ex;

-- In the default ignore mode the four numeric->temporal branches serve two callers with opposite
-- needs, so they key on the exact-target flag: an exact target (DROP/OPTIMIZE PARTITION, strict IN,
-- KeyCondition, sharding key) gets the canonical storage value, while value materialization clamps
-- like CAST. Both halves are asserted below for each of the four targets.
SELECT '-- exact target: a literal outside the storage type raises, so DROP PARTITION cannot drop the wrong partition';
SET date_time_overflow_behavior = 'ignore';
DROP TABLE IF EXISTS t_exact_dt;
CREATE TABLE t_exact_dt (k DateTime, v UInt8) ENGINE = MergeTree PARTITION BY k ORDER BY tuple();
INSERT INTO t_exact_dt SELECT toDateTime(100), 1;
INSERT INTO t_exact_dt SELECT toDateTime(4294967295), 2;
-- 99999999999 is outside UInt32, so no DateTime partition can hold it: raise instead of clamping to 4294967295.
ALTER TABLE t_exact_dt DROP PARTITION 99999999999; -- { serverError ARGUMENT_OUT_OF_BOUND }
OPTIMIZE TABLE t_exact_dt PARTITION 99999999999; -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT arraySort(groupArray(toUInt32(k))) FROM t_exact_dt;
-- the largest storable value still addresses its own partition
ALTER TABLE t_exact_dt DROP PARTITION 4294967295;
SELECT arraySort(groupArray(toUInt32(k))) FROM t_exact_dt;
DROP TABLE t_exact_dt;

DROP TABLE IF EXISTS t_exact_date;
CREATE TABLE t_exact_date (k Date, v UInt8) ENGINE = MergeTree PARTITION BY k ORDER BY tuple();
INSERT INTO t_exact_date SELECT toDate(19), 1;
INSERT INTO t_exact_date SELECT toDate(65535), 2;
ALTER TABLE t_exact_date DROP PARTITION 99999999999; -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT arraySort(groupArray(toUInt16(k))) FROM t_exact_date;
DROP TABLE t_exact_date;

DROP TABLE IF EXISTS t_exact_date32;
CREATE TABLE t_exact_date32 (k Date32, v UInt8) ENGINE = MergeTree PARTITION BY k ORDER BY tuple();
INSERT INTO t_exact_date32 SELECT toDate32(19), 1;
INSERT INTO t_exact_date32 SELECT toDate32(120000), 2;
ALTER TABLE t_exact_date32 DROP PARTITION 999999999999; -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT arraySort(groupArray(toInt32(k))) FROM t_exact_date32;
DROP TABLE t_exact_date32;

SELECT '-- exact target: a literal inside the storage type but outside the visible range stays addressable';
-- Time is backed by Int32 with a plain SerializationNumber, so 4000000 (inside Int32, outside the
-- visible Time range) can really be stored. Clamping the literal to 3599999 would drop the wrong
-- partition; rejecting it would leave its own partition undroppable. Both are asserted.
DROP TABLE IF EXISTS t_exact_time;
CREATE TABLE t_exact_time (k Time, v UInt8) ENGINE = MergeTree PARTITION BY k ORDER BY tuple();
INSERT INTO t_exact_time SELECT toTime(100), 1;
INSERT INTO t_exact_time SELECT toTime(3599999), 2;
ALTER TABLE t_exact_time DROP PARTITION 4000000;
SELECT arraySort(groupArray(toInt32(k))) FROM t_exact_time;
-- outside Int32 storage: still raises
ALTER TABLE t_exact_time DROP PARTITION 99999999999; -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT arraySort(groupArray(toInt32(k))) FROM t_exact_time;
DROP TABLE t_exact_time;

DROP TABLE IF EXISTS t_exact_time_own;
CREATE TABLE t_exact_time_own (k Time, v UInt8) ENGINE = MergeTree PARTITION BY k ORDER BY tuple();
INSERT INTO t_exact_time_own SELECT toTime(100), 1;
INSERT INTO t_exact_time_own VALUES (4000000, 2);
SELECT arraySort(groupArray(toInt32(k))) FROM t_exact_time_own;
ALTER TABLE t_exact_time_own DROP PARTITION 4000000;
SELECT arraySort(groupArray(toInt32(k))) FROM t_exact_time_own;
DROP TABLE t_exact_time_own;

SELECT '-- exact target: strict IN must not match an out-of-range literal against a boundary row';
SELECT toDateTime(4294967295) IN (99999999999), toTime(3599999) IN (4000000), toDate(65535) IN (99999999999), toDate32(120000) IN (999999999999);
SELECT toDateTime(4294967295) IN (4294967295), toTime(3599999) IN (3599999), toDate(65535) IN (65535), toDate32(120000) IN (120000);

SELECT '-- value materialization: an out-of-range literal clamps exactly like CAST';
SELECT toInt32(c), toInt32(CAST(99999999999 AS Time)) FROM values('c Time', 99999999999);
SELECT toString(c), toString(CAST(99999999999 AS Date)) FROM values('c Date', 99999999999);
SELECT toString(c), toString(CAST(999999999999 AS Date32)) FROM values('c Date32', 999999999999);
SELECT toString(c), toString(CAST(99999999999 AS DateTime)) FROM values('c DateTime', 99999999999);
SELECT toInt32(c), toInt32(CAST(-9000000000000000000 AS Time)) FROM values('c Time', -9000000000000000000);
SELECT toInt32(c), toInt32(CAST(4000000 AS Time)) FROM values('c Time', 4000000);

SELECT '-- value materialization: wide-integer and float sources clamp like CAST too';
SELECT toString(c), toString(CAST(toUInt128(99999999999) AS Date)) FROM values('c Date', toUInt128(99999999999));
SELECT toString(c), toString(CAST(toUInt256(99999999999) AS Date)) FROM values('c Date', toUInt256(99999999999));
SELECT toString(c), toString(CAST(toInt128(999999999999) AS Date32)) FROM values('c Date32', toInt128(999999999999));
SELECT toString(c), toString(CAST(toInt256(999999999999) AS Date32)) FROM values('c Date32', toInt256(999999999999));
SELECT toInt32(c), toInt32(CAST(toUInt128(99999999999) AS Time)) FROM values('c Time', toUInt128(99999999999));
SELECT toString(c), toString(CAST(toUInt128(99999999999) AS DateTime)) FROM values('c DateTime', toUInt128(99999999999));
SELECT toString(c), toString(CAST(1e30 AS Date)) FROM values('c Date', 1e30);
SELECT toInt32(c), toInt32(CAST(nan AS Time)) FROM values('c Time', nan);
SELECT toString(c), toString(CAST(inf AS DateTime)) FROM values('c DateTime', inf);

SELECT '-- UInt32 keeps clamping into Time; the UInt32 -> DateTime cast is unaffected (UInt32::max is the DateTime maximum)';
SELECT toInt32(CAST(toUInt32(4000000) AS Time)), toInt32(CAST(toUInt32(4294967295) AS Time));
SELECT toString(CAST(toUInt32(4294967295) AS DateTime)), toString(CAST(toUInt32(4000000) AS DateTime));
SELECT toInt32(CAST(c AS Time)), toString(CAST(c AS DateTime)) FROM (SELECT toUInt32(4294967295) AS c);
SELECT toInt32(CAST(toNullable(toUInt32(4000000)) AS Nullable(Time)));
SELECT toString(CAST(toUInt64(18446744073709551615) AS DateTime)), toString(CAST(toUInt128(18446744073709551615) + 1 AS DateTime));
