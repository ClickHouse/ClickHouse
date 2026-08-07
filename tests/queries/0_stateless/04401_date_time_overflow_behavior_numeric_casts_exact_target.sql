-- Companion to 04401_date_time_overflow_behavior_numeric_casts.sql, split off to keep each test
-- under the flaky check runtime cap. This half covers the exact-target callers of the numeric
-- temporal coercion: DROP/OPTIMIZE PARTITION, strict IN, and the window frame RANGE offset.
-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/101131

SET session_timezone = 'UTC';
SET allow_experimental_time_time64_type = 1;

-- In the default ignore mode the Date/Date32/Time coercion still rejects an out-of-storage-range
-- value (Null -> ARGUMENT_OUT_OF_BOUND in convertFieldToTypeOrThrow), so a DROP/OPTIMIZE PARTITION
-- with a bogus numeric literal cannot silently drop the wrong partition.
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
-- A non-finite float is unconvertible, so materialization rejects it exactly like CAST does, in every
-- overflow mode. Each row below selects ONLY the values() side: pairing it with a throwing CAST would
-- pass on the CAST's exception alone and never test what the column received.
SELECT toInt32(c) FROM values('c Time', nan); -- { serverError CANNOT_CONVERT_TYPE }
SELECT toString(c) FROM values('c DateTime', inf); -- { serverError CANNOT_CONVERT_TYPE }
SELECT toString(c) FROM values('c Date', nan); -- { serverError CANNOT_CONVERT_TYPE }
-- Date32 rejects a non-finite float like the other three targets: it used to be the one target that
-- saturated inf to 2299-12-31 and NaN to 1900-01-01 in every mode, so the same numeric CAST answered
-- CANNOT_CONVERT_TYPE for Date/DateTime/Time and a boundary date for Date32.
SELECT toString(c) FROM values('c Date32', -inf) SETTINGS date_time_overflow_behavior = 'saturate'; -- { serverError CANNOT_CONVERT_TYPE }
SELECT toString(CAST(inf AS Date32)) SETTINGS date_time_overflow_behavior = 'saturate'; -- { serverError CANNOT_CONVERT_TYPE }
SELECT toString(CAST(nan AS Date32)) SETTINGS date_time_overflow_behavior = 'ignore'; -- { serverError CANNOT_CONVERT_TYPE }
-- Control: a finite value outside the Date32 range still saturates, so the rows above cannot pass by
-- rejecting every float.
SELECT toString(CAST(1e30 AS Date32)) SETTINGS date_time_overflow_behavior = 'saturate';
SELECT toInt32(c) FROM values('c Time', nan) SETTINGS date_time_overflow_behavior = 'saturate'; -- { serverError CANNOT_CONVERT_TYPE }
SELECT toString(c) FROM values('c Date', nan) SETTINGS date_time_overflow_behavior = 'saturate'; -- { serverError CANNOT_CONVERT_TYPE }
SELECT toString(c) FROM values('c DateTime', inf) SETTINGS date_time_overflow_behavior = 'ignore'; -- { serverError CANNOT_CONVERT_TYPE }
-- Control: a finite out-of-range value still clamps, so the rows above cannot pass by rejecting everything.
SELECT toInt32(c) FROM values('c Time', 4000000.0) SETTINGS date_time_overflow_behavior = 'saturate';

SELECT '-- UInt32 keeps clamping into Time; the UInt32 -> DateTime cast is unaffected (UInt32::max is the DateTime maximum)';
SELECT toInt32(CAST(toUInt32(4000000) AS Time)), toInt32(CAST(toUInt32(4294967295) AS Time));
SELECT toString(CAST(toUInt32(4294967295) AS DateTime)), toString(CAST(toUInt32(4000000) AS DateTime));
SELECT toInt32(CAST(c AS Time)), toString(CAST(c AS DateTime)) FROM (SELECT toUInt32(4294967295) AS c);
SELECT toInt32(CAST(toNullable(toUInt32(4000000)) AS Nullable(Time)));
SELECT toString(CAST(toUInt64(18446744073709551615) AS DateTime)), toString(CAST(toUInt128(18446744073709551615) + 1 AS DateTime));

-- A window frame RANGE offset is a distance in the ORDER BY column's underlying units, not a temporal
-- point, so it must never be reinterpreted as a day number / unix timestamp nor clamped to the target's
-- visible range. WindowTransform coerces the offset through convertFieldToTypeOrThrow with
-- convert_inexact_floats = true, which stopped taking the plain storage-type arm once the temporal coerce
-- helpers appeared: a Date offset of 65536 was silently read as a timestamp and became a ~0-day frame
-- instead of being rejected, and a Time offset above MAX_TIME_TIMESTAMP was clamped, shrinking the frame.
-- Both are wrong results, so the offset now always takes the storage-type arm in every overflow mode.
DROP TABLE IF EXISTS t_win_date;
DROP TABLE IF EXISTS t_win_time;
DROP TABLE IF EXISTS t_win_date32;
DROP TABLE IF EXISTS t_win_datetime;
CREATE TABLE t_win_date (d Date) ENGINE = Memory;
INSERT INTO t_win_date VALUES ('2020-01-01'), ('2020-01-02'), ('2020-01-03');
CREATE TABLE t_win_time (t Time) ENGINE = Memory;
INSERT INTO t_win_time VALUES (-200000), (0), (3599999);
-- Date32 day numbers spanning the extended day-number boundary (DATE_LUT_MAX_EXTEND_DAY_NUM = 120530),
-- so an offset above it discriminates the storage-type arm from the timestamp reinterpretation.
CREATE TABLE t_win_date32 (d Date32) ENGINE = Memory;
INSERT INTO t_win_date32 VALUES (toDate32(-25567)), (toDate32(0)), (toDate32(120000));
CREATE TABLE t_win_datetime (d DateTime) ENGINE = Memory;
INSERT INTO t_win_datetime VALUES (toDateTime(0)), (toDateTime(100)), (toDateTime(4294967295));

SELECT '-- window frame offset: ignore mode';
SET date_time_overflow_behavior = 'ignore';
-- 65536 is outside the UInt16 storage of Date, so the offset is rejected in every mode.
SELECT d, count() OVER (ORDER BY d RANGE BETWEEN 65536 PRECEDING AND CURRENT ROW) FROM t_win_date ORDER BY d; -- { serverError ARGUMENT_OUT_OF_BOUND }
-- A Time offset of 4000000 seconds is a legitimate distance and fits Int32, so it must be taken verbatim.
-- Asserted through frame membership: verbatim reaches -400001 from the last row and includes -200000
-- (3 rows), while clamping the offset to 3599999 would reach 0 and exclude it (2 rows).
SELECT toInt32(t), count() OVER (ORDER BY t RANGE BETWEEN 4000000 PRECEDING AND CURRENT ROW) FROM t_win_time ORDER BY t;
-- A Date32 offset of 150000 days is a legitimate distance and fits Int32, so it must be taken verbatim.
-- 150000 is above DATE_LUT_MAX_EXTEND_DAY_NUM, so reading it as a unix timestamp instead would turn it
-- into ~1 day and shrink the frame to 1 row on every row; verbatim it reaches every earlier row (1/2/3).
SELECT toInt32(d), count() OVER (ORDER BY d RANGE BETWEEN 150000 PRECEDING AND CURRENT ROW) FROM t_win_date32 ORDER BY d;
-- A DateTime offset of 2147483646 seconds is a legitimate distance inside both Int32 and UInt32, so it is
-- taken verbatim and reaches the 0 and 100 rows from the 4294967295 row is not (2147483646 < 4294967195).
SELECT toUInt32(d), count() OVER (ORDER BY d RANGE BETWEEN 2147483646 PRECEDING AND CURRENT ROW) FROM t_win_datetime ORDER BY d;
-- In-range controls, so the rows above cannot pass by rejecting or shrinking everything.
SELECT d, count() OVER (ORDER BY d RANGE BETWEEN 1 PRECEDING AND CURRENT ROW) FROM t_win_date ORDER BY d;
SELECT toInt32(t), count() OVER (ORDER BY t RANGE BETWEEN 200000 PRECEDING AND CURRENT ROW) FROM t_win_time ORDER BY t;
SELECT toInt32(d), count() OVER (ORDER BY d RANGE BETWEEN 100 PRECEDING AND CURRENT ROW) FROM t_win_date32 ORDER BY d;
SELECT toUInt32(d), count() OVER (ORDER BY d RANGE BETWEEN 100 PRECEDING AND CURRENT ROW) FROM t_win_datetime ORDER BY d;

SELECT '-- window frame offset: saturate mode';
SET date_time_overflow_behavior = 'saturate';
SELECT d, count() OVER (ORDER BY d RANGE BETWEEN 65536 PRECEDING AND CURRENT ROW) FROM t_win_date ORDER BY d; -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT toInt32(t), count() OVER (ORDER BY t RANGE BETWEEN 4000000 PRECEDING AND CURRENT ROW) FROM t_win_time ORDER BY t;
SELECT toInt32(d), count() OVER (ORDER BY d RANGE BETWEEN 150000 PRECEDING AND CURRENT ROW) FROM t_win_date32 ORDER BY d;
SELECT toUInt32(d), count() OVER (ORDER BY d RANGE BETWEEN 2147483646 PRECEDING AND CURRENT ROW) FROM t_win_datetime ORDER BY d;
SELECT d, count() OVER (ORDER BY d RANGE BETWEEN 1 PRECEDING AND CURRENT ROW) FROM t_win_date ORDER BY d;
SELECT toInt32(t), count() OVER (ORDER BY t RANGE BETWEEN 200000 PRECEDING AND CURRENT ROW) FROM t_win_time ORDER BY t;
SELECT toInt32(d), count() OVER (ORDER BY d RANGE BETWEEN 100 PRECEDING AND CURRENT ROW) FROM t_win_date32 ORDER BY d;
SELECT toUInt32(d), count() OVER (ORDER BY d RANGE BETWEEN 100 PRECEDING AND CURRENT ROW) FROM t_win_datetime ORDER BY d;

SELECT '-- window frame offset: throw mode';
SET date_time_overflow_behavior = 'throw';
SELECT d, count() OVER (ORDER BY d RANGE BETWEEN 65536 PRECEDING AND CURRENT ROW) FROM t_win_date ORDER BY d; -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT toInt32(t), count() OVER (ORDER BY t RANGE BETWEEN 4000000 PRECEDING AND CURRENT ROW) FROM t_win_time ORDER BY t;
SELECT toInt32(d), count() OVER (ORDER BY d RANGE BETWEEN 150000 PRECEDING AND CURRENT ROW) FROM t_win_date32 ORDER BY d;
SELECT toUInt32(d), count() OVER (ORDER BY d RANGE BETWEEN 2147483646 PRECEDING AND CURRENT ROW) FROM t_win_datetime ORDER BY d;
SELECT d, count() OVER (ORDER BY d RANGE BETWEEN 1 PRECEDING AND CURRENT ROW) FROM t_win_date ORDER BY d;
SELECT toInt32(t), count() OVER (ORDER BY t RANGE BETWEEN 200000 PRECEDING AND CURRENT ROW) FROM t_win_time ORDER BY t;
SELECT toInt32(d), count() OVER (ORDER BY d RANGE BETWEEN 100 PRECEDING AND CURRENT ROW) FROM t_win_date32 ORDER BY d;
SELECT toUInt32(d), count() OVER (ORDER BY d RANGE BETWEEN 100 PRECEDING AND CURRENT ROW) FROM t_win_datetime ORDER BY d;
SET date_time_overflow_behavior = 'ignore';

DROP TABLE t_win_date;
DROP TABLE t_win_time;
DROP TABLE t_win_date32;
DROP TABLE t_win_datetime;
