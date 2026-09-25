-- `toStartOfInterval` returns a value of the type of its argument, see
-- https://github.com/ClickHouse/ClickHouse/pull/117396#issuecomment-5823121011

SET session_timezone = 'UTC';
SET to_start_of_interval_preserves_argument_type = 1;

SELECT '-- Date32 values out of the range of Date and DateTime';
DROP TABLE IF EXISTS t32;
CREATE TABLE t32 (d Date32) ENGINE = MergeTree ORDER BY d
    SETTINGS index_granularity = 1, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;
INSERT INTO t32 VALUES ('1900-01-01'), ('1969-12-31'), ('2000-01-01'), ('2149-06-06'), ('2200-01-01'), ('2299-12-31');

SELECT d, toStartOfInterval(d, INTERVAL 1 DAY) AS x, toTypeName(x) FROM t32 ORDER BY d;
SELECT d, toStartOfInterval(d, INTERVAL 1 WEEK) AS x, toStartOfInterval(d, INTERVAL 1 YEAR) AS y, toTypeName(y) FROM t32 ORDER BY d;

SELECT '-- primary key analysis';
SELECT count() FROM t32 WHERE toStartOfInterval(d, INTERVAL 1 DAY) >= toDate32('2050-01-01');
SELECT count() FROM t32 WHERE toStartOfInterval(d, INTERVAL 1 YEAR) >= toDate32('2100-01-01');
SELECT count() FROM t32 WHERE toStartOfInterval(d, INTERVAL 1 WEEK) < toDate32('1970-01-01');
DROP TABLE t32;

SELECT '-- Date';
SELECT toStartOfInterval(toDate('2023-05-17'), INTERVAL 1 DAY) AS x, toTypeName(x);
SELECT toStartOfInterval(toDate('2023-05-17'), INTERVAL 10 DAY) AS x, toTypeName(x);
SELECT toStartOfInterval(toDate('2023-05-17'), INTERVAL 1 WEEK) AS x, toTypeName(x);
SELECT toStartOfInterval(toDate('2023-05-17'), INTERVAL 1 MONTH) AS x, toTypeName(x);
SELECT toStartOfInterval(toDate('2023-05-17'), INTERVAL 1 QUARTER) AS x, toTypeName(x);
SELECT toStartOfInterval(toDate('2023-05-17'), INTERVAL 1 YEAR) AS x, toTypeName(x);
-- A time zone argument has no effect on a `Date`.
SELECT toStartOfInterval(toDate('2023-05-17'), INTERVAL 1 DAY, 'Asia/Tokyo') AS x, toTypeName(x);
SELECT toStartOfInterval(toDate('2023-05-17'), INTERVAL 1 WEEK, 'Asia/Tokyo') AS x, toTypeName(x);
-- Rounding the first days of the epoch down to a week reaches before the range of `Date`; the result is clamped.
SELECT toStartOfInterval(toDate('1970-01-01') + number, INTERVAL 1 WEEK) FROM numbers(6);
SELECT toStartOfInterval(toDate('2149-06-06'), INTERVAL 1 DAY), toStartOfInterval(toDate('2149-06-06'), INTERVAL 1 YEAR);

SELECT '-- DateTime';
SELECT toStartOfInterval(toDateTime('2023-05-17 12:34:56', 'Asia/Tokyo'), INTERVAL 10 SECOND) AS x, toTypeName(x);
SELECT toStartOfInterval(toDateTime('2023-05-17 12:34:56', 'Asia/Tokyo'), INTERVAL 15 MINUTE) AS x, toTypeName(x);
SELECT toStartOfInterval(toDateTime('2023-05-17 12:34:56', 'Asia/Tokyo'), INTERVAL 1 HOUR) AS x, toTypeName(x);
SELECT toStartOfInterval(toDateTime('2023-05-17 12:34:56', 'Asia/Tokyo'), INTERVAL 1 DAY) AS x, toTypeName(x);
SELECT toStartOfInterval(toDateTime('2023-05-17 12:34:56', 'Asia/Tokyo'), INTERVAL 1 WEEK) AS x, toTypeName(x);
SELECT toStartOfInterval(toDateTime('2023-05-17 12:34:56', 'Asia/Tokyo'), INTERVAL 1 MONTH) AS x, toTypeName(x);
SELECT toStartOfInterval(toDateTime('2023-05-17 12:34:56', 'Asia/Tokyo'), INTERVAL 1 QUARTER) AS x, toTypeName(x);
SELECT toStartOfInterval(toDateTime('2023-05-17 12:34:56', 'Asia/Tokyo'), INTERVAL 1 YEAR) AS x, toTypeName(x);
SELECT toStartOfInterval(toDateTime('2023-05-17 12:34:56', 'Asia/Tokyo'), INTERVAL 1 MONTH, 'America/New_York') AS x, toTypeName(x);
SELECT toStartOfInterval(toDateTime('1970-01-01 00:00:00', 'UTC') + number * 86400, INTERVAL 1 WEEK) FROM numbers(6);

SELECT '-- DateTime64';
SELECT toStartOfInterval(toDateTime64('2023-05-17 12:34:56.123456', 6, 'Asia/Tokyo'), INTERVAL 1 MILLISECOND) AS x, toTypeName(x);
SELECT toStartOfInterval(toDateTime64('2023-05-17 12:34:56.123456', 6, 'Asia/Tokyo'), INTERVAL 100 MICROSECOND) AS x, toTypeName(x);
SELECT toStartOfInterval(toDateTime64('2023-05-17 12:34:56.123456', 6, 'Asia/Tokyo'), INTERVAL 1 NANOSECOND) AS x, toTypeName(x);
SELECT toStartOfInterval(toDateTime64('2023-05-17 12:34:56.123', 3, 'Asia/Tokyo'), INTERVAL 10 SECOND) AS x, toTypeName(x);
SELECT toStartOfInterval(toDateTime64('2023-05-17 12:34:56.123', 3, 'Asia/Tokyo'), INTERVAL 1 HOUR) AS x, toTypeName(x);
SELECT toStartOfInterval(toDateTime64('2023-05-17 12:34:56.123', 3, 'Asia/Tokyo'), INTERVAL 1 DAY) AS x, toTypeName(x);
SELECT toStartOfInterval(toDateTime64('2023-05-17 12:34:56.123', 3, 'Asia/Tokyo'), INTERVAL 1 WEEK) AS x, toTypeName(x);
SELECT toStartOfInterval(toDateTime64('2023-05-17 12:34:56.123', 3, 'Asia/Tokyo'), INTERVAL 1 YEAR) AS x, toTypeName(x);
SELECT toStartOfInterval(toDateTime64('2023-05-17 12:34:56', 0, 'Asia/Tokyo'), INTERVAL 1 MONTH) AS x, toTypeName(x);
SELECT toStartOfInterval(toDateTime64('2023-05-17 12:34:56.123456789', 9, 'Asia/Tokyo'), INTERVAL 1 MONTH) AS x, toTypeName(x);
-- Values before the epoch and after the range of `DateTime`.
SELECT toStartOfInterval(toDateTime64('1969-12-31 23:59:59', 3), INTERVAL 1 DAY), toStartOfInterval(toDateTime64('1969-12-31 23:59:59', 3), INTERVAL 1 MINUTE);
SELECT toStartOfInterval(toDateTime64('1900-01-01 12:34:56.789', 3), INTERVAL 1 HOUR), toStartOfInterval(toDateTime64('1900-03-11 12:34:56.789', 3), INTERVAL 1 MONTH);
SELECT toStartOfInterval(toDateTime64('2299-12-31 12:34:56.789', 3), INTERVAL 1 SECOND), toStartOfInterval(toDateTime64('2299-12-31 12:34:56.789', 3), INTERVAL 1 WEEK);
-- Non-constant arguments, which take the arithmetic fast path for the units up to an hour.
SELECT toStartOfInterval(toDateTime64('2023-05-17 12:34:56.789', 3) + toIntervalSecond(number * 1000), INTERVAL 1 HOUR) FROM numbers(5);
SELECT toStartOfInterval(toDateTime64('1969-12-31 23:00:00', 3) + toIntervalSecond(number * 1000), INTERVAL 15 MINUTE) FROM numbers(5);
SELECT toStartOfInterval(toDateTime64('2023-05-17 12:34:56.789', 3) + toIntervalSecond(number), INTERVAL 1 SECOND) FROM numbers(3);
-- `Asia/Kolkata` is not hour-aligned, and its offset has seconds before 1906, which take the generic path row by row.
SELECT toStartOfInterval(toDateTime64('1900-01-02 03:04:05.678', 3, 'Asia/Kolkata') + toIntervalDay(number * 10000), INTERVAL 1 HOUR) FROM numbers(4);
SELECT toStartOfInterval(toDateTime64('1900-01-02 03:04:05.678', 3, 'Asia/Kolkata') + toIntervalDay(number * 10000), INTERVAL 1 MINUTE) FROM numbers(4);
-- The fast path gives the same results as the dedicated functions. The values are whole seconds, because before the
-- epoch the fractional part of a `DateTime64` is truncated towards zero, which is a separate issue.
SELECT countIf(toStartOfInterval(t, INTERVAL 1 MINUTE) != toStartOfMinute(t))
     + countIf(toStartOfInterval(t, INTERVAL 1 HOUR) != toStartOfHour(t))
     + countIf(toStartOfInterval(t, INTERVAL 1 SECOND) != toStartOfSecond(t))
FROM (SELECT toDateTime64('1960-01-01 00:00:00', 3, 'Europe/Amsterdam') + toIntervalSecond(number * 123457) AS t FROM numbers(10000))
SETTINGS enable_extended_results_for_datetime_functions = 1;

SELECT '-- the origin overload';
SELECT toStartOfInterval(toDateTime64('2023-01-01 00:00:00.123456', 6), INTERVAL 1 MILLISECOND, toDateTime64('2023-01-01 00:00:00', 6)) AS x, toTypeName(x);
SELECT toStartOfInterval(toDateTime64('2023-01-01 14:45:00.5', 1), INTERVAL 1 MINUTE, toDateTime64('2023-01-01 14:35:30', 1)) AS x, toTypeName(x);
-- The origin has a part finer than the interval unit, and the grid anchored at it is kept in the scale of the argument.
SELECT toStartOfInterval(toDateTime64('2023-01-01 00:00:00.000001700', 9), INTERVAL 1 MICROSECOND, toDateTime64('2023-01-01 00:00:00.000000500', 9)) AS x, toTypeName(x);
SELECT toStartOfInterval(toDateTime64('1969-12-31 23:59:59.999998700', 9), INTERVAL 2 MICROSECOND, toDateTime64('1969-12-31 23:59:59.000000500', 9)) AS x, toTypeName(x);
SELECT reinterpret(toStartOfInterval(reinterpret(toInt64(-9223372036854775800), 'DateTime64(9)'), toIntervalMicrosecond(2), reinterpret(toInt64(-9223372036854775808), 'DateTime64(9)')), 'Int64');
SELECT toStartOfInterval(toDate('2023-01-17'), INTERVAL 1 WEEK, toDate('2023-01-03')) AS x, toTypeName(x);

SELECT '-- dateTrunc keeps its own result type';
SELECT toTypeName(dateTrunc('day', toDate('2023-05-17'))), toTypeName(dateTrunc('week', toDateTime('2023-05-17 12:34:56'))), toTypeName(dateTrunc('hour', toDateTime64('2023-05-17 12:34:56.123', 3)));

SELECT '-- legacy behavior';
SET to_start_of_interval_preserves_argument_type = 0;
SELECT toStartOfInterval(toDate('2023-05-17'), INTERVAL 1 DAY) AS x, toTypeName(x);
SELECT toStartOfInterval(toDateTime('2023-05-17 12:34:56'), INTERVAL 1 WEEK) AS x, toTypeName(x);
SELECT toStartOfInterval(toDateTime64('2023-05-17 12:34:56.123', 3), INTERVAL 1 HOUR) AS x, toTypeName(x);
SELECT toStartOfInterval(toDateTime64('2023-05-17 12:34:56.123', 3), INTERVAL 1 MICROSECOND) AS x, toTypeName(x);
SELECT toStartOfInterval(toDate32('1900-01-01'), INTERVAL 1 DAY) AS x, toTypeName(x);
SELECT toStartOfInterval(toDate('2023-05-17'), INTERVAL 1 WEEK, 'Asia/Tokyo'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT toStartOfInterval(toDate32('1900-01-01'), INTERVAL 1 DAY) AS x, toTypeName(x) SETTINGS enable_extended_results_for_datetime_functions = 1;
