-- `toSecond` and the `toStartOf*` rounding take an arithmetic fast path when the time zone's offset is a
-- whole number of minutes (or hours) *during the epoch*, and applied it to pre-1970 values as well. In a
-- zone whose historical offset has a sub-minute component - `Europe/Amsterdam` was +00:19:32 until 1937,
-- `Asia/Kolkata` +05:21:10 until 1906 - the answer was the second of the UTC minute, and the rounding
-- landed on a UTC-aligned boundary, disagreeing with `toString` of the very same value.

SET enable_extended_results_for_datetime_functions = 1;

SELECT 'a pre-1937 Amsterdam timestamp';
SELECT toString(toDateTime64('1930-06-15 12:00:34', 0, 'Europe/Amsterdam')) AS rendered,
       toSecond(toDateTime64('1930-06-15 12:00:34', 0, 'Europe/Amsterdam')) AS second,
       toMinute(toDateTime64('1930-06-15 12:00:34', 0, 'Europe/Amsterdam')) AS minute,
       toHour(toDateTime64('1930-06-15 12:00:34', 0, 'Europe/Amsterdam')) AS hour;
SELECT toStartOfMinute(toDateTime64('1930-06-15 12:00:34', 0, 'Europe/Amsterdam')),
       toStartOfFiveMinutes(toDateTime64('1930-06-15 12:00:34', 0, 'Europe/Amsterdam')),
       toStartOfFifteenMinutes(toDateTime64('1930-06-15 12:16:34', 0, 'Europe/Amsterdam')),
       toStartOfHour(toDateTime64('1930-06-15 12:30:00', 0, 'Europe/Amsterdam'));
SELECT toStartOfInterval(toDateTime64('1930-06-15 12:00:34', 0, 'Europe/Amsterdam'), INTERVAL 1 MINUTE),
       toStartOfInterval(toDateTime64('1930-06-15 12:00:34', 0, 'Europe/Amsterdam'), INTERVAL 5 MINUTE),
       toStartOfInterval(toDateTime64('1930-06-15 12:30:00', 0, 'Europe/Amsterdam'), INTERVAL 1 HOUR),
       toStartOfInterval(toDateTime64('1930-06-15 12:00:34', 0, 'Europe/Amsterdam'), INTERVAL 30 SECOND);

SELECT 'a pre-1906 Kolkata timestamp';
SELECT toString(toDateTime64('1902-06-15 12:00:34', 0, 'Asia/Kolkata')) AS rendered,
       toSecond(toDateTime64('1902-06-15 12:00:34', 0, 'Asia/Kolkata')) AS second;
SELECT toStartOfMinute(toDateTime64('1902-06-15 12:00:34', 0, 'Asia/Kolkata'));

SELECT 'the same zone after the transition, where the fast path is exact';
SELECT toSecond(toDateTime64('1950-06-15 12:00:34', 0, 'Europe/Amsterdam')),
       toStartOfMinute(toDateTime64('1950-06-15 12:00:34', 0, 'Europe/Amsterdam')),
       toStartOfHour(toDateTime64('1950-06-15 12:30:34', 0, 'Europe/Amsterdam'));
SELECT toSecond(toDateTime('2024-06-15 12:00:34', 'Europe/Amsterdam')),
       toStartOfMinute(toDateTime('2024-06-15 12:00:34', 'Europe/Amsterdam')),
       toStartOfHour(toDateTime('2024-06-15 12:30:34', 'Europe/Amsterdam')),
       toStartOfInterval(toDateTime('2024-06-15 12:30:34', 'Europe/Amsterdam'), INTERVAL 1 HOUR);

SELECT 'and UTC, which never had a sub-minute offset';
SELECT toSecond(toDateTime64('1930-06-15 12:00:34', 0, 'UTC')),
       toStartOfMinute(toDateTime64('1930-06-15 12:00:34', 0, 'UTC')),
       toStartOfHour(toDateTime64('1930-06-15 12:30:34', 0, 'UTC')),
       toStartOfInterval(toDateTime64('1930-06-15 12:30:34', 0, 'UTC'), INTERVAL 1 HOUR);

SELECT 'a column mixing values on both sides of the epoch, where `toStartOfInterval` takes the fast path';
SELECT toString(t), toString(toStartOfInterval(t, INTERVAL 1 MINUTE)), toString(toStartOfInterval(t, INTERVAL 1 HOUR))
FROM
(
    SELECT toDateTime64('1930-06-15 12:00:34', 0, 'Europe/Amsterdam') + number * 2874009634 AS t
    FROM numbers(3)
)
ORDER BY t;
