-- The IN operator uses exact value semantics (the `strict` mode of `convertFieldToType`):
-- a set element that is not exactly representable in the left-hand type must be excluded
-- from the set instead of being truncated or wrapped into a spurious match.
SET enable_time_time64_type = 1;
SET session_timezone = 'UTC';
SET use_legacy_to_time = 0;
SET async_insert = 0;

SELECT 'Time64 -> Time';
SELECT CAST('00:00:00', 'Time') IN (CAST('00:00:00.1', 'Time64(1)'));
SELECT CAST('00:00:00', 'Time') IN (CAST('00:00:00.0', 'Time64(1)'));
SELECT CAST('01:02:03', 'Time') IN (CAST('01:02:03.000', 'Time64(3)'));
SELECT CAST('-01:02:03', 'Time') IN (CAST('-01:02:03.5', 'Time64(1)'));

SELECT 'Time64 -> DateTime';
SELECT toDateTime(1) IN (CAST('00:00:01.5', 'Time64(1)'));
SELECT toDateTime(1) IN (CAST('00:00:01', 'Time64(0)'));
-- A negative time-of-day would wrap to the end of the DateTime range under the default
-- `date_time_overflow_behavior = 'ignore'`; strict IN must reject it instead.
SELECT toDateTime(4294967295) IN (CAST('-00:00:01', 'Time64(0)'));

SELECT 'Time -> DateTime';
SELECT toDateTime(3723) IN (CAST('01:02:03', 'Time'));
SELECT toDateTime(4294967295) IN (CAST('-00:00:01', 'Time'));

SELECT 'Time -> Date';
-- Truncating the time-of-day would match 1970-01-01; strict IN must reject it.
SELECT toDate('1970-01-01') IN (CAST('01:02:03', 'Time'));
SELECT toDate('1970-01-01') IN (CAST('00:00:00', 'Time'));
SELECT toDate('1970-01-02') IN (CAST('24:00:00', 'Time'));
SELECT toDate('1970-01-01') IN (CAST('-01:00:00', 'Time'));

SELECT 'Time -> Date32';
SELECT toDate32('1969-12-31') IN (CAST('-01:00:00', 'Time'));
SELECT toDate32('1969-12-31') IN (CAST('-24:00:00', 'Time'));
SELECT toDate32('1970-01-01') IN (CAST('00:00:01', 'Time'));

SELECT 'Time64 -> Date';
SELECT toDate('1970-01-01') IN (CAST('00:00:00.5', 'Time64(1)'));
SELECT toDate('1970-01-01') IN (CAST('00:00:00.0', 'Time64(1)'));
SELECT toDate('1970-01-02') IN (CAST('24:00:00.000', 'Time64(3)'));

SELECT 'Time64 -> Date32';
SELECT toDate32('1969-12-31') IN (CAST('-00:00:00.5', 'Time64(1)'));
SELECT toDate32('1969-12-31') IN (CAST('-24:00:00.0', 'Time64(1)'));

SELECT 'INSERT VALUES still converts lossily';
-- The VALUES path is non-strict: it must keep mirroring the column path (CAST) instead of
-- rejecting values that lose precision.
CREATE TEMPORARY TABLE t_strict_in (t Time, d Date, dt DateTime) ENGINE = Memory;
INSERT INTO t_strict_in VALUES (CAST('01:02:03.5', 'Time64(1)'), CAST('01:02:03', 'Time'), CAST('00:00:01.5', 'Time64(1)'));
SELECT * FROM t_strict_in;
