-- Day/week addition on DateTime/DateTime64 takes an arithmetic fast path in fixed-offset time zones.
-- Verify that the results are identical to the calendar semantics and that DST zones keep the calendar path.

SELECT 'DateTime, UTC';
SELECT addDays(toDateTime('2020-02-28 23:30:00', 'UTC'), 1);
SELECT addDays(toDateTime('2020-02-28 23:30:00', 'UTC'), 2);
SELECT addDays(toDateTime('2020-03-01 00:15:30', 'UTC'), -1);
SELECT addWeeks(toDateTime('2020-02-28 23:30:00', 'UTC'), 1);
SELECT subtractDays(toDateTime('2020-03-01 00:15:30', 'UTC'), 1);
SELECT subtractWeeks(toDateTime('2020-03-05 12:00:00', 'UTC'), 1);
SELECT toDateTime('2020-12-31 23:59:59', 'UTC') + INTERVAL 1 DAY;
SELECT toDateTime('2021-01-07 00:00:00', 'UTC') - INTERVAL 1 WEEK;

SELECT 'DateTime, fixed non-UTC offset';
SELECT addDays(toDateTime('2037-12-31 23:59:59', 'Etc/GMT-5'), 1);
SELECT addWeeks(toDateTime('2020-02-25 01:02:03', 'Etc/GMT+5'), 1);
SELECT subtractDays(toDateTime('1970-01-02 05:00:00', 'Etc/GMT-5'), 1);

SELECT 'DateTime64, UTC';
SELECT addDays(toDateTime64('1950-01-01 00:00:00.123', 3, 'UTC'), 40);
SELECT addDays(toDateTime64('1900-01-15 12:00:00.500', 3, 'UTC'), -14);
SELECT addWeeks(toDateTime64('2299-12-01 00:00:00.999', 3, 'UTC'), 4);
SELECT subtractDays(toDateTime64('1970-01-01 00:00:00.000', 3, 'UTC'), 1);
SELECT subtractWeeks(toDateTime64('2020-02-29 23:59:59.999999', 6, 'UTC'), 1);
SELECT toDateTime64('2262-04-11 23:47:16.854775807', 9, 'UTC') - INTERVAL 1 DAY;

SELECT 'Equivalence with addSeconds in a fixed-offset time zone';
SELECT sum(addDays(t, n) != addSeconds(t, n * 86400)) + sum(addWeeks(t, n) != addSeconds(t, n * 604800))
FROM (SELECT toDateTime(1000000000 + number * 7777, 'UTC') AS t, toInt64(number % 61) - 30 AS n FROM numbers(100000));
SELECT sum(addDays(t, n) != addSeconds(t, n * 86400)) + sum(addWeeks(t, n) != addSeconds(t, n * 604800))
FROM (SELECT toDateTime64(1000000000.123 + number * 7777.001, 3, 'Etc/GMT-5') AS t, toInt64(number % 61) - 30 AS n FROM numbers(100000));

SELECT 'Non-constant delta and non-constant time';
SELECT addDays(toDateTime('2020-02-28 23:30:00', 'UTC'), number) FROM numbers(3);
SELECT addDays(materialize(toDateTime('2020-02-28 23:30:00', 'UTC')), 1);
SELECT addWeeks(materialize(toDateTime64('2020-02-28 23:30:00.25', 2, 'UTC')), number) FROM numbers(3);

SELECT 'Boundaries keep the calendar-path behavior in fixed-offset time zones';
-- When the input or the result leaves the range of the date LUT, the calendar path recomputes the
-- value with cctz and saturates it to the representable calendar [0000, 9999]; the fast path must
-- fall back to it. Text output does not distinguish every result, so also pin the raw values.
SELECT subtractDays(toDateTime64('1900-01-01 00:00:00', 0, 'UTC'), 1) AS x, reinterpretAsInt64(x);
SELECT subtractDays(materialize(toDateTime64('1900-01-01 00:00:00', 0, 'UTC')), 1) AS x, reinterpretAsInt64(x);
SELECT subtractDays(toDateTime64('1900-01-01 12:00:00.5', 1, 'UTC'), 2) AS x, reinterpretAsInt64(x);
SELECT subtractWeeks(toDateTime64('1900-01-01 00:00:00', 0, 'UTC'), 1) AS x, reinterpretAsInt64(x);
SELECT subtractDays(toDateTime64('1900-01-01 00:00:00', 0, 'Etc/GMT-5'), 1) AS x, reinterpretAsInt64(x);
SELECT addDays(toDateTime64('2299-12-31 23:59:59.999', 3, 'UTC'), 2) AS x, reinterpretAsInt64(x);
SELECT addWeeks(toDateTime64('2299-12-31 00:00:00', 0, 'Etc/GMT+5'), 52) AS x, reinterpretAsInt64(x);
-- Negative sub-second values shifted to the upper LUT edge: the calendar path truncates the division
-- towards zero, so the fast path must decline near the edge to keep the results identical.
SELECT addDays(toDateTime64('1969-12-31 23:59:59.999', 3, 'UTC'), 120530) AS x, reinterpretAsInt64(x);
SELECT addDays(materialize(toDateTime64('1969-12-31 23:59:59.999', 3, 'UTC')), 120530) AS x, reinterpretAsInt64(x);
SELECT addWeeks(toDateTime64('1969-12-28 23:59:59.999', 3, 'UTC'), 17219) AS x, reinterpretAsInt64(x);
SELECT addWeeks(materialize(toDateTime64('1969-12-28 23:59:59.999', 3, 'UTC')), 17219) AS x, reinterpretAsInt64(x);
-- Huge deltas take the calendar path as well.
SELECT addDays(toDateTime64('2020-01-01 00:00:00', 0, 'UTC'), 9223372036854775807) AS x, reinterpretAsInt64(x);
SELECT subtractDays(toDateTime64('2020-01-01 00:00:00', 0, 'UTC'), 9223372036854775807) AS x, reinterpretAsInt64(x);
SELECT addWeeks(toDateTime64('2020-01-01 00:00:00', 0, 'UTC'), 9223372036854775807) AS x, reinterpretAsInt64(x);
-- Subtracting INT64_MIN: the negation wraps, and the wrapped delta must take the calendar path.
SELECT subtractDays(toDateTime64('2020-01-01 00:00:00', 0, 'UTC'), CAST('-9223372036854775808', 'Int64')) AS x, reinterpretAsInt64(x);
SELECT subtractWeeks(toDateTime64('2020-01-01 00:00:00', 0, 'UTC'), CAST('-9223372036854775808', 'Int64')) AS x, reinterpretAsInt64(x);
SELECT subtractWeeks(materialize(toDateTime64('2020-01-01 00:00:00', 0, 'UTC')), CAST('-9223372036854775808', 'Int64')) AS x, reinterpretAsInt64(x);
SELECT subtractWeeks(toDateTime('2020-01-01 00:00:00', 'UTC'), CAST('-9223372036854775808', 'Int64')) AS x, toUInt32(x);
-- DateTime whose result leaves the LUT range.
SELECT addDays(toDateTime('2020-01-01 00:00:00', 'UTC'), 10000000) AS x, toUInt32(x);
SELECT addWeeks(toDateTime('2020-01-01 00:00:00', 'UTC'), 10000000) AS x, toUInt32(x);
SELECT addWeeks(toDateTime('2020-01-01 00:00:00', 'UTC'), 9223372036854775807) AS x, toUInt32(x);

SELECT 'DST time zone keeps calendar semantics';
-- Crossing the spring-forward transition (2021-03-28 in Europe/Berlin): the day is 23 hours long.
SELECT addDays(toDateTime('2021-03-27 12:00:00', 'Europe/Berlin'), 1);
SELECT toUInt32(addDays(toDateTime('2021-03-27 12:00:00', 'Europe/Berlin'), 1)) - toUInt32(toDateTime('2021-03-27 12:00:00', 'Europe/Berlin'));
-- Crossing the fall-back transition (2021-10-31 in Europe/Berlin): the day is 25 hours long.
SELECT addDays(toDateTime('2021-10-30 12:00:00', 'Europe/Berlin'), 1);
SELECT toUInt32(addDays(toDateTime('2021-10-30 12:00:00', 'Europe/Berlin'), 1)) - toUInt32(toDateTime('2021-10-30 12:00:00', 'Europe/Berlin'));
SELECT addWeeks(toDateTime64('2021-03-24 12:00:00.5', 1, 'Europe/Berlin'), 1);
SELECT subtractDays(toDateTime('2021-03-29 12:00:00', 'Europe/Berlin'), 2);
-- In a DST zone addDays must not degenerate to adding 86400 seconds.
SELECT addDays(toDateTime('2021-03-27 12:00:00', 'Europe/Berlin'), 1) = addSeconds(toDateTime('2021-03-27 12:00:00', 'Europe/Berlin'), 86400);
