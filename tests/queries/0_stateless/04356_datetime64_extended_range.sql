-- Tests for DateTime64 values outside of the former [1900, 2299] range.
-- DateLUTImpl now falls back to cctz for out-of-range time points.

SET session_timezone = 'UTC';
-- So that toStartOf* return the wide DateTime64 / Date32 types instead of DateTime / Date.
SET enable_extended_results_for_datetime_functions = 1;

-- Round-trip of string representation, before 1900 and after 2299.
SELECT toDateTime64('1850-03-15 12:34:56.789', 3, 'UTC');
SELECT toDateTime64('1023-08-09 01:02:03.000', 3, 'UTC');
SELECT toDateTime64('2500-06-15 23:45:01.500', 3, 'UTC');
SELECT toDateTime64('9999-12-31 23:59:59.999', 3, 'UTC');
SELECT toDateTime64('0001-01-01 00:00:00.000', 3, 'UTC');

-- Component extraction for out-of-range values.
SELECT toYear(d), toMonth(d), toDayOfMonth(d), toHour(d), toMinute(d), toSecond(d), toDayOfWeek(d)
FROM (SELECT toDateTime64('1850-03-15 12:34:56', 0, 'UTC') AS d);
SELECT toYear(d), toMonth(d), toDayOfMonth(d), toHour(d)
FROM (SELECT toDateTime64('2500-06-15 23:45:01', 0, 'UTC') AS d);

-- Rounding functions (extended results -> Date32 / DateTime64).
SELECT toStartOfYear(toDateTime64('1850-07-15 12:00:00', 0, 'UTC'));
SELECT toStartOfMonth(toDateTime64('2500-07-15 12:00:00', 0, 'UTC'));
SELECT toStartOfDay(toDateTime64('1700-02-28 18:30:00', 0, 'UTC'));
SELECT toStartOfHour(toDateTime64('2400-02-29 18:30:45', 0, 'UTC')); -- 2400 is a leap year
SELECT date_trunc('month', toDateTime64('1599-11-30 05:06:07', 0, 'UTC'));

-- Calendar arithmetic crossing the 1900 / 2300 boundaries.
SELECT toDateTime64('2299-06-15 12:00:00', 0, 'UTC') + INTERVAL 10 YEAR;
SELECT toDateTime64('1900-03-31 00:00:00', 0, 'UTC') - INTERVAL 1 MONTH;
SELECT addYears(toDateTime64('1850-02-28 00:00:00', 0, 'UTC'), 4);
SELECT toDateTime64('1850-01-01 00:00:00', 0, 'UTC') + INTERVAL 100000 DAY;

-- Date/time difference across the boundary.
SELECT dateDiff('year', toDateTime64('1850-01-01 00:00:00', 0, 'UTC'), toDateTime64('2350-01-01 00:00:00', 0, 'UTC'));

-- Ordering is preserved for out-of-range values.
SELECT toDateTime64('1850-01-01 00:00:00', 0, 'UTC') < toDateTime64('1851-01-01 00:00:00', 0, 'UTC');
SELECT toDateTime64('2500-01-01 00:00:00', 0, 'UTC') > toDateTime64('2299-01-01 00:00:00', 0, 'UTC');

-- Self-checking round-trips: parse, format, and parse again must agree.
SELECT count() = countIf(toString(d) = s)
FROM
(
    SELECT s, toDateTime64(s, 0, 'UTC') AS d
    FROM values('s String', '1599-12-31 23:59:59', '1700-01-01 00:00:00', '1899-12-31 23:59:59',
                '1900-01-01 00:00:00', '2299-12-31 23:59:59', '2300-01-01 00:00:00', '3000-06-15 12:00:00')
);

-- toDateTime64 from a numeric Unix timestamp beyond 2299.
SELECT toDateTime64(16725225600, 0, 'UTC'); -- 2500-01-01 00:00:00 UTC
