-- Using strings containing a date and time as elements of an `IN` set for a `Date`/`Date32` column.
-- A follow-up to https://github.com/ClickHouse/ClickHouse/pull/111000 which fixed comparison operators.

-- The time zone is pinned because the test framework randomizes `session_timezone`,
-- and some time zones have DST transitions at midnight.
SET session_timezone = 'UTC';

SELECT 'Date IN strings with a date and time';
SELECT toDate('2026-01-01') IN ('2026-01-01 00:00:00');
SELECT toDate('2026-01-01') IN ('2026-01-01 12:00:00');
SELECT toDate('2026-01-01') IN ('2026-01-02 00:00:00');
SELECT toDate('2026-01-01') IN '2026-01-01 00:00:00';
SELECT toDate('2026-01-01') NOT IN ('2026-01-01 00:00:00');
SELECT toDate('2026-01-01') NOT IN ('2026-01-01 12:00:00');

SELECT 'Fractional seconds';
SELECT toDate('2026-01-01') IN ('2026-01-01 00:00:00.000');
SELECT toDate('2026-01-01') IN ('2026-01-01 00:00:00.500');

SELECT 'Date32';
SELECT toDate32('2026-01-01') IN ('2026-01-01 00:00:00');
SELECT toDate32('2026-01-01') IN ('2026-01-01 12:00:00');
SELECT toDate32('1950-06-15') IN ('1950-06-15 00:00:00');
SELECT toDate32('1950-06-15') IN ('1950-06-15 12:00:00');
SELECT toDate32('1950-06-15') NOT IN ('1950-06-15 00:00:00');

SELECT 'Dates out of the range of the column type never match';
SELECT toDate('2026-01-01') IN ('1950-06-15 00:00:00');

SELECT 'Mixed lists';
SELECT toDate('2026-01-01') IN ('2026-01-01 00:00:00', '2025-05-05');
SELECT toDate('2026-01-01') IN ('2026-01-01 12:00:00', '2026-01-01');
SELECT toDate('2026-01-01') IN ('2026-01-01 12:00:00', '2025-05-05');
SELECT toDate('2026-01-01') IN ('2026-01-02 00:00:00', toDate('2026-01-01'));

SELECT 'Non-constant date column';
SELECT materialize(toDate('2026-01-01')) IN ('2026-01-01 00:00:00');
SELECT materialize(toDate('2026-01-01')) IN ('2026-01-01 12:00:00');
SELECT materialize(toDate32('2026-01-01')) IN ('2026-01-01 00:00:00');

SELECT 'Nullable';
SELECT toNullable(toDate('2026-01-01')) IN ('2026-01-01 00:00:00');
SELECT toNullable(toDate('2026-01-01')) IN ('2026-01-01 12:00:00');

SELECT 'Strings without a time still work';
SELECT toDate('2026-01-01') IN ('2026-01-01');
SELECT toDate('2026-01-01') IN ('2026-01-02');

SELECT 'Filtering by a Date key column';
DROP TABLE IF EXISTS t_date_in_datetime_strings;
CREATE TABLE t_date_in_datetime_strings (d Date) ENGINE = MergeTree ORDER BY d;
INSERT INTO t_date_in_datetime_strings VALUES ('2025-12-31'), ('2026-01-01'), ('2026-01-02');
SELECT count() FROM t_date_in_datetime_strings WHERE d IN ('2026-01-01 00:00:00');
SELECT count() FROM t_date_in_datetime_strings WHERE d IN ('2026-01-01 12:00:00');
SELECT count() FROM t_date_in_datetime_strings WHERE d IN ('2026-01-01 00:00:00', '2026-01-02 00:00:00');
SELECT count() FROM t_date_in_datetime_strings WHERE d NOT IN ('2026-01-01 00:00:00');
DROP TABLE t_date_in_datetime_strings;

SELECT 'Invalid strings still throw';
SELECT toDate('2026-01-01') IN ('garbage'); -- { serverError CANNOT_PARSE_DATE }
SELECT toDate32('2026-01-01') IN ('garbage'); -- { serverError CANNOT_PARSE_DATE }
SELECT toDate('2026-01-01') IN ('2026-01-01 12:00:00 garbage'); -- { serverError TYPE_MISMATCH }
SELECT toDate('2026-01-01') IN ('2026-01-01 00:00:00', 'garbage'); -- { serverError CANNOT_PARSE_DATE }
