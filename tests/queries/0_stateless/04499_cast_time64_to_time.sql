-- Casting Time64 to Time was never implemented and failed with CANNOT_CONVERT_TYPE.
SET enable_time_time64_type = 1;

-- Direct cast: Time64 -> Time keeps the seconds and drops the fractional part.
SELECT CAST(CAST('10:00:00' AS Time64) AS Time) = CAST('10:00:00' AS Time);
SELECT CAST(CAST('10:00:00.987' AS Time64(3)) AS Time) = CAST('10:00:00' AS Time);
SELECT toTypeName(CAST(CAST('10:00:00' AS Time64) AS Time));

-- accurateCastOrNull must also succeed and never null out a valid value.
SELECT accurateCastOrNull(CAST('01:02:03.5' AS Time64(1)), 'Time') = CAST('01:02:03' AS Time);

-- Out-of-range Time64 (built via Decimal64 -> Time64, which does not clamp) must clamp the stored value.
SELECT CAST(CAST(toDecimal64(3600001, 0) AS Time64(0)) AS Time) = CAST(3599999 AS Time);
SELECT CAST(CAST(toDecimal64(-3600001, 0) AS Time64(0)) AS Time) = CAST(-3599999 AS Time);

-- Under throw mode the same out-of-range value must raise instead of clamping, while in-range still converts.
SELECT CAST(CAST(toDecimal64(3600001, 0) AS Time64(0)) AS Time) SETTINGS date_time_overflow_behavior = 'throw'; -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT CAST(CAST('10:00:00' AS Time64) AS Time) = CAST('10:00:00' AS Time) SETTINGS date_time_overflow_behavior = 'throw';

-- Constant Time64 inserted into a Time column goes through convertFieldToType, which must drop the fraction too.
DROP TABLE IF EXISTS t_04499_time;
CREATE TABLE t_04499_time (c1 Time) ENGINE = Memory;
INSERT INTO t_04499_time VALUES (CAST('01:02:03.5' AS Time64(1)));
SELECT c1 = CAST('01:02:03' AS Time) FROM t_04499_time;
DROP TABLE t_04499_time;

-- Strict callers (IN-set construction) must reject a non-representable Time64 element, not truncate it.
SELECT CAST('01:02:03' AS Time) IN (CAST('01:02:03.5' AS Time64(1)));
SELECT CAST('01:02:03' AS Time) IN (CAST('01:02:03.0' AS Time64(1)));

-- Out-of-range constant Time64 into a Time column must honor date_time_overflow_behavior = 'throw'.
DROP TABLE IF EXISTS t_04499_time2;
CREATE TABLE t_04499_time2 (c1 Time) ENGINE = Memory;
INSERT INTO t_04499_time2 SETTINGS date_time_overflow_behavior = 'throw' VALUES (CAST(toDecimal64(3600001, 0) AS Time64(0))); -- { error VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
DROP TABLE t_04499_time2;

-- Nested Time64 values must convert too: the source type hint is propagated into Array/Tuple/Map.
DROP TABLE IF EXISTS t_04499_time3;
CREATE TABLE t_04499_time3 (a Array(Time), t Tuple(Time, Int32), m Map(String, Time)) ENGINE = Memory;
INSERT INTO t_04499_time3 VALUES ([CAST('01:02:03.5' AS Time64(1))], (CAST('01:02:03.5' AS Time64(1)), 42), map('k', CAST('01:02:03.5' AS Time64(1))));
SELECT a = [CAST('01:02:03' AS Time)], t.1 = CAST('01:02:03' AS Time), m['k'] = CAST('01:02:03' AS Time) FROM t_04499_time3;
DROP TABLE t_04499_time3;

-- Negative fractional values round toward negative infinity, like DateTime64 conversions.
SELECT CAST(CAST(toDecimal64(-0.5, 1) AS Time64(1)) AS Time) = CAST(-1 AS Time);

-- Same for constants, and Nullable(Time64) source hints are unwrapped, including nested ones.
DROP TABLE IF EXISTS t_04499_time4;
CREATE TABLE t_04499_time4 (c1 Time, a Array(Time)) ENGINE = Memory;
INSERT INTO t_04499_time4 VALUES (CAST(toDecimal64(-0.5, 1) AS Time64(1)), [CAST('02:00:00.5' AS Nullable(Time64(1)))]) (CAST('01:02:03.5' AS Nullable(Time64(1))), []);
SELECT c1 = CAST(-1 AS Time), c1 = CAST('01:02:03' AS Time), a = [CAST('02:00:00' AS Time)] FROM t_04499_time4 ORDER BY c1;
DROP TABLE t_04499_time4;

-- Nullable(Tuple(...)) source hints are unwrapped before propagating element types.
SET enable_nullable_tuple_type = 1;
DROP TABLE IF EXISTS t_04499_time5;
CREATE TABLE t_04499_time5 (t Tuple(Time, UInt8)) ENGINE = Memory;
INSERT INTO t_04499_time5 VALUES (CAST((CAST('01:02:03.5' AS Time64(1)), 1) AS Nullable(Tuple(Time64(1), UInt8))));
SELECT t.1 = CAST('01:02:03' AS Time), t.2 FROM t_04499_time5;
DROP TABLE t_04499_time5;

-- Subquery sets: probing a Time set with a sub-second Time64 must not match, mirroring DateTime64.
SELECT CAST('01:02:03.5' AS Time64(1)) IN (SELECT CAST('01:02:03' AS Time));
SELECT CAST('01:02:03.0' AS Time64(1)) IN (SELECT CAST('01:02:03' AS Time));

-- Nullable probes must be precision-filtered too; with transform_null_in they reach the set unwrapped.
SELECT CAST('01:02:03.5' AS Nullable(Time64(1))) IN (SELECT CAST('01:02:03' AS Time));
SELECT CAST('01:02:03.5' AS Nullable(Time64(1))) IN (SELECT CAST('01:02:03' AS Time)) SETTINGS transform_null_in = 1;
SELECT materialize(CAST('2020-01-01 00:00:00.5' AS Nullable(DateTime64(1)))) IN (SELECT CAST('2020-01-01 00:00:00' AS DateTime)) SETTINGS transform_null_in = 1;
