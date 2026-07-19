-- Comparing Date/Date32 with a String containing a date and time.
-- https://github.com/ClickHouse/ClickHouse/issues/99372

-- The time zone is pinned because the test framework randomizes `session_timezone`,
-- and some time zones have DST transitions at midnight.
SET session_timezone = 'UTC';

SELECT 'Date vs a string with a date and time';
SELECT toDate('2026-01-01') < '2026-01-01 12:00:00';
SELECT toDate('2026-01-01') > '2026-01-01 12:00:00';
SELECT toDate('2026-01-01') <= '2026-01-01 00:00:00';
SELECT toDate('2026-01-01') >= '2026-01-01 00:00:00';
SELECT toDate('2026-01-01') = '2026-01-01 00:00:00';
SELECT toDate('2026-01-01') != '2026-01-01 00:00:00';
SELECT toDate('2026-01-01') = '2026-01-01 12:00:00';
SELECT toDate('2026-01-01') != '2026-01-01 12:00:00';
SELECT toDate('2026-01-02') > '2026-01-01 23:59:59';

SELECT 'String on the left side';
SELECT '2026-01-01 12:00:00' > toDate('2026-01-01');
SELECT '2026-01-01 12:00:00' < toDate('2026-01-01');
SELECT '2026-01-01 00:00:00' = toDate('2026-01-01');
SELECT '2026-01-01 12:00:00' != toDate('2026-01-01');

SELECT 'Date32';
SELECT toDate32('2026-01-01') < '2026-01-01 12:00:00';
SELECT toDate32('2026-01-01') = '2026-01-01 00:00:00';
SELECT toDate32('2026-01-01') = '2026-01-01 12:00:00';
SELECT '2026-01-01 12:00:00' > toDate32('2026-01-01');
SELECT toDate32('1950-06-15') = '1950-06-15 00:00:00';
SELECT toDate32('1950-06-15') < '1950-06-15 12:00:00';

SELECT 'Fractional seconds';
SELECT toDate('2026-01-01') < '2026-01-01 00:00:00.500';
SELECT toDate('2026-01-01') = '2026-01-01 00:00:00.000';
SELECT toDate('2026-01-01') = '2026-01-01 00:00:00.500';
SELECT '2026-01-01 00:00:00.500' > toDate('2026-01-01');

SELECT 'Non-constant date column';
SELECT materialize(toDate('2026-01-01')) < '2026-01-01 12:00:00';
SELECT materialize(toDate('2026-01-01')) = '2026-01-01 00:00:00';
SELECT materialize(toDate32('2026-01-01')) < '2026-01-01 12:00:00';
SELECT '2026-01-01 12:00:00' > materialize(toDate('2026-01-01'));

SELECT 'Strings without a time still work';
SELECT toDate('2026-01-01') = '2026-01-01';
SELECT toDate('2026-01-01') < '2026-01-02';
SELECT toDateTime('2026-01-01 00:00:00') = '2026-01-01';

SELECT 'Filtering by a Date key column';
DROP TABLE IF EXISTS t_date_vs_datetime_string;
CREATE TABLE t_date_vs_datetime_string (d Date) ENGINE = MergeTree ORDER BY d;
INSERT INTO t_date_vs_datetime_string VALUES ('2025-12-31'), ('2026-01-01'), ('2026-01-02');
SELECT count() FROM t_date_vs_datetime_string WHERE d < '2026-01-01 12:00:00';
SELECT count() FROM t_date_vs_datetime_string WHERE d = '2026-01-01 00:00:00';
SELECT count() FROM t_date_vs_datetime_string WHERE d > '2026-01-01 12:00:00';
DROP TABLE t_date_vs_datetime_string;

-- A bloom filter skip index on a `Date` column must not fail the query either.
DROP TABLE IF EXISTS t_date_bloom_filter;
CREATE TABLE t_date_bloom_filter (d Date, INDEX bf d TYPE bloom_filter GRANULARITY 1) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_date_bloom_filter VALUES ('2026-01-01'), ('2026-01-02'), ('2026-01-03');
SELECT count() FROM t_date_bloom_filter WHERE d = '2026-01-02 00:00:00';
DROP TABLE t_date_bloom_filter;

SELECT 'Invalid strings still throw';
SELECT toDate('2026-01-01') = 'garbage'; -- { serverError CANNOT_PARSE_DATE }
SELECT toDate32('2026-01-01') = 'garbage'; -- { serverError CANNOT_PARSE_DATE }
SELECT toDate('2026-01-01') < '2026-01-01 12:00:00 garbage'; -- { serverError TYPE_MISMATCH }
