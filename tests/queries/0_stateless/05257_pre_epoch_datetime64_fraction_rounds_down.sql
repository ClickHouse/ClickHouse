-- Rounding a `DateTime64` before the epoch with a fractional part down to a minute, an hour, a day or another
-- interval must not move it forward in time: the whole seconds are rounded towards negative infinity.
-- https://github.com/ClickHouse/ClickHouse/issues/119967

SET session_timezone = 'UTC';
SET enable_extended_results_for_datetime_functions = 1;

SELECT '-- toStartOf*';
SELECT toStartOfMinute(toDateTime64('1969-12-31 23:58:59.500', 3));
SELECT toStartOfFiveMinutes(toDateTime64('1969-12-31 23:54:59.500', 3));
SELECT toStartOfTenMinutes(toDateTime64('1969-12-31 23:49:59.500', 3));
SELECT toStartOfFifteenMinutes(toDateTime64('1969-12-31 23:44:59.500', 3));
SELECT toStartOfHour(toDateTime64('1969-12-31 22:59:59.500', 3));
SELECT toStartOfDay(toDateTime64('1969-12-31 23:59:59.500', 3));
SELECT timeSlot(toDateTime64('1969-12-31 23:29:59.500', 3));
SELECT toDate32(toDateTime64('1969-12-31 23:59:59.500', 3)), toStartOfMonth(toDateTime64('1969-12-31 23:59:59.500', 3));
SELECT toStartOfMinute(toDateTime64('1969-12-31 23:58:59.500', 3)) > toDateTime64('1969-12-31 23:58:59.500', 3);

SELECT '-- the same one-second shift with the default result types';
SELECT toTimeWithFixedDate(toDateTime64('1969-12-31 23:59:59.000', 3)), toTimeWithFixedDate(toDateTime64('1969-12-31 23:59:59.500', 3))
SETTINGS enable_extended_results_for_datetime_functions = 0;

SELECT '-- toStartOfInterval';
SELECT toStartOfInterval(toDateTime64('1969-12-31 23:59:59.500', 3), INTERVAL 1 SECOND);
SELECT toStartOfInterval(toDateTime64('1969-12-31 23:58:59.500', 3), INTERVAL 1 MINUTE);
SELECT toStartOfInterval(toDateTime64('1969-12-31 22:59:59.500', 3), INTERVAL 1 HOUR);
SELECT toStartOfInterval(toDateTime64('1969-12-31 23:59:59.500', 3), INTERVAL 1 DAY);
SELECT toStartOfInterval(toDateTime64('1970-01-04 23:59:59.500', 3) - toIntervalWeek(1), INTERVAL 1 WEEK);
SELECT toStartOfInterval(toDateTime64('1969-12-31 23:59:59.500', 3), INTERVAL 1 MONTH);
SELECT toStartOfInterval(toDateTime64('1969-12-31 23:59:59.500', 3), INTERVAL 1 QUARTER);
SELECT toStartOfInterval(toDateTime64('1969-12-31 23:59:59.500', 3), INTERVAL 1 YEAR);
SELECT dateTrunc('minute', toDateTime64('1969-12-31 23:58:59.500', 3)), dateTrunc('day', toDateTime64('1969-12-31 23:59:59.500', 3));

SELECT '-- non-constant arguments, which take the arithmetic fast path of toStartOfInterval';
SELECT toStartOfInterval(toDateTime64('1969-12-31 23:58:58.500', 3) + toIntervalMillisecond(number * 250), INTERVAL 1 SECOND) FROM numbers(6);
SELECT toStartOfInterval(toDateTime64('1969-12-31 23:58:59.500', 3) + toIntervalMillisecond(number * 250), INTERVAL 1 MINUTE) FROM numbers(4);
SELECT toStartOfInterval(toDateTime64('1969-12-31 22:59:59.999999', 6, 'Asia/Kolkata') + toIntervalMicrosecond(number), INTERVAL 1 HOUR) FROM numbers(2);

SELECT '-- the start of an interval is never later than the value';
SELECT countIf(toStartOfInterval(t, INTERVAL 1 SECOND) > t) + countIf(toStartOfInterval(t, INTERVAL 1 MINUTE) > t)
     + countIf(toStartOfInterval(t, INTERVAL 1 HOUR) > t) + countIf(toStartOfInterval(t, INTERVAL 1 DAY) > t)
     + countIf(toStartOfMinute(t) > t) + countIf(toStartOfHour(t) > t) + countIf(toStartOfDay(t) > t)
     + countIf(toStartOfInterval(t, INTERVAL 1 SECOND) != toStartOfSecond(t))
FROM (SELECT toDateTime64('1960-01-01 00:00:00', 3, 'Europe/Amsterdam') + toIntervalMillisecond(number * 123456789) AS t FROM numbers(10000));

SELECT '-- values after the epoch are unchanged';
SELECT toStartOfMinute(toDateTime64('1970-01-01 00:00:59.500', 3)), toStartOfInterval(toDateTime64('1970-01-01 00:00:59.500', 3), INTERVAL 1 MINUTE);
