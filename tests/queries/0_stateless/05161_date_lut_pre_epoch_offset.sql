-- `Europe/Moscow` was +02:30:17 until 1919: inside the table but before the epoch, where the fast paths
-- gated on offset properties sampled from the epoch onwards must not be taken.
SELECT
    toString(t, 'Europe/Moscow') AS local,
    toHour(t, 'Europe/Moscow') AS h,
    toMinute(t, 'Europe/Moscow') AS m,
    toSecond(t, 'Europe/Moscow') AS s,
    toString(date_trunc('hour', t), 'Europe/Moscow') AS start_of_hour,
    toString(date_trunc('minute', t), 'Europe/Moscow') AS start_of_minute
FROM (SELECT toDateTime64(-2195911170, 0, 'Europe/Moscow') AS t);

-- The same over the early years of the table: the accessors must match what `toString` prints, and
-- truncating must land on a local boundary within one interval. `Asia/Kolkata` was +05:21:10 until 1906 and
-- `Europe/Amsterdam` +00:19:32 until 1937; `UTC` is whole throughout, so it must keep its fast paths.
CREATE TEMPORARY TABLE pre_epoch AS
    SELECT toDateTime64(-2208988800 + number * 1013, 0, 'UTC') AS t FROM numbers(100000);

SELECT 'Europe/Moscow',
    countIf(toHour(t, 'Europe/Moscow') != toUInt8(substring(toString(t, 'Europe/Moscow'), 12, 2))) AS wrong_hour,
    countIf(toMinute(t, 'Europe/Moscow') != toUInt8(substring(toString(t, 'Europe/Moscow'), 15, 2))) AS wrong_minute,
    countIf(toSecond(t, 'Europe/Moscow') != toUInt8(substring(toString(t, 'Europe/Moscow'), 18, 2))) AS wrong_second,
    countIf(substring(toString(date_trunc('hour', t, 'Europe/Moscow'), 'Europe/Moscow'), 15, 5) != '00:00') AS hour_off_boundary,
    countIf(toInt64(t) - toInt64(date_trunc('hour', t, 'Europe/Moscow')) NOT BETWEEN 0 AND 3599) AS hour_too_far,
    countIf(substring(toString(date_trunc('minute', t, 'Europe/Moscow'), 'Europe/Moscow'), 18, 2) != '00') AS minute_off_boundary,
    countIf(toInt64(t) - toInt64(date_trunc('minute', t, 'Europe/Moscow')) NOT BETWEEN 0 AND 59) AS minute_too_far
FROM pre_epoch;

SELECT 'Asia/Kolkata',
    countIf(toHour(t, 'Asia/Kolkata') != toUInt8(substring(toString(t, 'Asia/Kolkata'), 12, 2))) AS wrong_hour,
    countIf(toMinute(t, 'Asia/Kolkata') != toUInt8(substring(toString(t, 'Asia/Kolkata'), 15, 2))) AS wrong_minute,
    countIf(toSecond(t, 'Asia/Kolkata') != toUInt8(substring(toString(t, 'Asia/Kolkata'), 18, 2))) AS wrong_second,
    countIf(substring(toString(date_trunc('hour', t, 'Asia/Kolkata'), 'Asia/Kolkata'), 15, 5) != '00:00') AS hour_off_boundary,
    countIf(toInt64(t) - toInt64(date_trunc('hour', t, 'Asia/Kolkata')) NOT BETWEEN 0 AND 3599) AS hour_too_far,
    countIf(substring(toString(date_trunc('minute', t, 'Asia/Kolkata'), 'Asia/Kolkata'), 18, 2) != '00') AS minute_off_boundary,
    countIf(toInt64(t) - toInt64(date_trunc('minute', t, 'Asia/Kolkata')) NOT BETWEEN 0 AND 59) AS minute_too_far
FROM pre_epoch;

SELECT 'Europe/Amsterdam',
    countIf(toHour(t, 'Europe/Amsterdam') != toUInt8(substring(toString(t, 'Europe/Amsterdam'), 12, 2))) AS wrong_hour,
    countIf(toMinute(t, 'Europe/Amsterdam') != toUInt8(substring(toString(t, 'Europe/Amsterdam'), 15, 2))) AS wrong_minute,
    countIf(toSecond(t, 'Europe/Amsterdam') != toUInt8(substring(toString(t, 'Europe/Amsterdam'), 18, 2))) AS wrong_second,
    countIf(substring(toString(date_trunc('hour', t, 'Europe/Amsterdam'), 'Europe/Amsterdam'), 15, 5) != '00:00') AS hour_off_boundary,
    countIf(toInt64(t) - toInt64(date_trunc('hour', t, 'Europe/Amsterdam')) NOT BETWEEN 0 AND 3599) AS hour_too_far,
    countIf(substring(toString(date_trunc('minute', t, 'Europe/Amsterdam'), 'Europe/Amsterdam'), 18, 2) != '00') AS minute_off_boundary,
    countIf(toInt64(t) - toInt64(date_trunc('minute', t, 'Europe/Amsterdam')) NOT BETWEEN 0 AND 59) AS minute_too_far
FROM pre_epoch;

SELECT 'UTC',
    countIf(toHour(t, 'UTC') != toUInt8(substring(toString(t, 'UTC'), 12, 2))) AS wrong_hour,
    countIf(toMinute(t, 'UTC') != toUInt8(substring(toString(t, 'UTC'), 15, 2))) AS wrong_minute,
    countIf(toSecond(t, 'UTC') != toUInt8(substring(toString(t, 'UTC'), 18, 2))) AS wrong_second,
    countIf(substring(toString(date_trunc('hour', t, 'UTC'), 'UTC'), 15, 5) != '00:00') AS hour_off_boundary,
    countIf(toInt64(t) - toInt64(date_trunc('hour', t, 'UTC')) NOT BETWEEN 0 AND 3599) AS hour_too_far,
    countIf(substring(toString(date_trunc('minute', t, 'UTC'), 'UTC'), 18, 2) != '00') AS minute_off_boundary,
    countIf(toInt64(t) - toInt64(date_trunc('minute', t, 'UTC')) NOT BETWEEN 0 AND 59) AS minute_too_far
FROM pre_epoch;

-- An interval that does not divide a day, before the epoch. `UTC` stays on the arithmetic path and rounds
-- from the epoch; `Europe/Moscow` goes through the table and rounds from the start of the local day.
SET enable_extended_results_for_datetime_functions = 1;
SELECT toString(toStartOfInterval(toDateTime64('1969-12-31 23:59:58', 0, 'UTC'), INTERVAL 7 SECOND), 'UTC') AS utc_7s,
       toString(toStartOfInterval(toDateTime64('1900-06-01 11:10:47', 0, 'Europe/Moscow'), INTERVAL 7 SECOND), 'Europe/Moscow') AS moscow_7s,
       toString(toStartOfInterval(toDateTime64('1900-06-01 11:10:47', 0, 'Europe/Moscow'), INTERVAL 5 MINUTE), 'Europe/Moscow') AS moscow_5m;

-- The reported reproductions: `Europe/Amsterdam` was +00:19:32 until 1937 and `Asia/Kolkata` +05:21:10 until
-- 1906, so the second of a pre-1970 timestamp must agree with the one `toString` renders, and truncating must
-- land on a local wall-clock boundary rather than a UTC one shifted by the modern offset.
SELECT
    toString(toDateTime64('1930-06-15 12:00:00', 0, 'Europe/Amsterdam')) AS rendered,
    toSecond(toDateTime64('1930-06-15 12:00:00', 0, 'Europe/Amsterdam')) AS amsterdam_second,
    toSecond(toDateTime64('1902-06-15 12:00:00', 0, 'Asia/Kolkata')) AS kolkata_second,
    toString(toStartOfMinute(toDateTime64('1930-06-15 12:00:34', 0, 'Europe/Amsterdam'))) AS start_of_minute,
    toString(toStartOfHour(toDateTime64('1930-06-15 12:30:00', 0, 'Europe/Amsterdam'))) AS start_of_hour,
    toString(toStartOfFiveMinutes(toDateTime64('1930-06-15 12:00:34', 0, 'Europe/Amsterdam'))) AS start_of_five_minutes;

-- Controls: `UTC` is whole throughout, and `Europe/Amsterdam` is whole again after 1937.
SELECT
    toSecond(toDateTime64('1930-06-15 12:00:34', 0, 'UTC')) AS utc_second,
    toString(toStartOfMinute(toDateTime64('1930-06-15 12:00:34', 0, 'UTC'))) AS utc_start_of_minute,
    toSecond(toDateTime64('1950-06-15 12:00:00', 0, 'Europe/Amsterdam')) AS amsterdam_after_1937;

-- A pre-epoch `DateTime64` with a sub-second part must round down to the start of its interval, never past
-- its own value, for every unit.
SELECT
    toString(toStartOfInterval(toDateTime64('1969-12-31 23:59:59.500', 3, 'UTC'), INTERVAL 1 SECOND)) AS second,
    toString(toStartOfInterval(toDateTime64('1969-12-31 23:58:59.500', 3, 'UTC'), INTERVAL 1 MINUTE)) AS minute,
    toString(toStartOfInterval(toDateTime64('1969-12-31 23:54:59.500', 3, 'UTC'), INTERVAL 5 MINUTE)) AS five_minutes,
    toString(toStartOfInterval(toDateTime64('1969-12-31 22:59:59.500', 3, 'UTC'), INTERVAL 1 HOUR)) AS hour,
    toString(toStartOfInterval(toDateTime64('1969-12-31 23:59:59.500', 3, 'UTC'), INTERVAL 1 DAY)) AS day,
    toString(toStartOfInterval(toDateTime64('1969-12-31 23:59:59.500', 3, 'UTC'), INTERVAL 1 MONTH)) AS month,
    toString(toStartOfInterval(toDateTime64('1969-12-31 23:59:59.500', 3, 'UTC'), INTERVAL 1 YEAR)) AS year;

-- `date_trunc` resolves to `toStartOfInterval`, so it must round the same way, and `Europe/Amsterdam` puts the
-- local boundary where the UTC one is not.
SELECT
    toString(date_trunc('minute', toDateTime64('1969-12-31 23:59:59.500', 3, 'UTC'))) AS minute,
    toString(date_trunc('hour', toDateTime64('1969-12-31 23:59:59.500', 3, 'UTC'))) AS hour,
    toString(date_trunc('day', toDateTime64('1969-12-31 23:59:59.500', 3, 'UTC'))) AS day,
    toString(date_trunc('hour', toDateTime64('1930-06-15 12:59:59.500', 3, 'Europe/Amsterdam')), 'Europe/Amsterdam') AS hour_amsterdam,
    toString(toStartOfInterval(toDateTime64('1930-06-15 12:59:59.500', 3, 'Europe/Amsterdam'), INTERVAL 1 HOUR), 'Europe/Amsterdam') AS interval_hour_amsterdam,
    toString(toStartOfInterval(toDateTime64('1930-06-15 12:04:59.500', 3, 'Europe/Amsterdam'), INTERVAL 5 MINUTE), 'Europe/Amsterdam') AS interval_five_minutes_amsterdam;

-- The same, swept over the early years of the table in a zone that keeps the arithmetic path and one that
-- does not, at both scales: the result must never be later than its own argument.
SELECT
    countIf(toStartOfInterval(t64, INTERVAL 7 SECOND) > t64) AS utc_7s,
    countIf(toStartOfInterval(t64, INTERVAL 1 MINUTE) > t64) AS utc_1m,
    countIf(toStartOfInterval(t64, INTERVAL 1 HOUR) > t64) AS utc_1h,
    countIf(toStartOfInterval(k64, INTERVAL 7 SECOND) > k64) AS kolkata_7s,
    countIf(toStartOfInterval(k64, INTERVAL 1 MINUTE) > k64) AS kolkata_1m,
    countIf(toStartOfInterval(k64, INTERVAL 1 HOUR) > k64) AS kolkata_1h
FROM (
    SELECT toDateTime64(-2208988800 + number * 1013, 3, 'UTC') + INTERVAL 500 MILLISECOND AS t64,
           toDateTime64(-2208988800 + number * 1013, 3, 'Asia/Kolkata') + INTERVAL 500 MILLISECOND AS k64
    FROM numbers(100000));

-- The same for the direct rounding functions. Splitting a negative `DateTime64` into components truncates
-- towards zero, which moves the value into the future, so the last fractional second before a boundary used
-- to be attributed to the next minute / hour / day. `1969-12-31 23:59:59.500` is a boundary for every unit,
-- and `Europe/Amsterdam` covers the case where the local boundary is not a UTC one.
SELECT
    toString(toStartOfMinute(toDateTime64('1969-12-31 23:59:59.500', 3, 'UTC'))) AS minute,
    toString(toStartOfFiveMinutes(toDateTime64('1969-12-31 23:59:59.500', 3, 'UTC'))) AS five_minutes,
    toString(toStartOfTenMinutes(toDateTime64('1969-12-31 23:59:59.500', 3, 'UTC'))) AS ten_minutes,
    toString(toStartOfFifteenMinutes(toDateTime64('1969-12-31 23:59:59.500', 3, 'UTC'))) AS fifteen_minutes,
    toString(toStartOfHour(toDateTime64('1969-12-31 23:59:59.500', 3, 'UTC'))) AS hour,
    toString(toStartOfDay(toDateTime64('1969-12-31 23:59:59.500', 3, 'UTC'))) AS day,
    toString(timeSlot(toDateTime64('1969-12-31 23:59:59.500', 3, 'UTC'))) AS time_slot,
    toString(toTimeWithFixedDate(toDateTime64('1969-12-31 23:59:59.500', 3, 'UTC'))) AS time_with_fixed_date;

SELECT
    toString(toStartOfMinute(toDateTime64('1930-06-15 12:59:59.500', 3, 'Europe/Amsterdam')), 'Europe/Amsterdam') AS minute,
    toString(toStartOfHour(toDateTime64('1930-06-15 12:59:59.500', 3, 'Europe/Amsterdam')), 'Europe/Amsterdam') AS hour,
    toString(toStartOfDay(toDateTime64('1930-06-15 23:59:59.500', 3, 'Europe/Amsterdam')), 'Europe/Amsterdam') AS day,
    toString(toStartOfFiveMinutes(toDateTime64('1930-06-15 12:04:59.500', 3, 'Europe/Amsterdam')), 'Europe/Amsterdam') AS five_minutes;

-- Swept over the early years of the table: the result must never be later than its own argument, and adding a
-- sub-second part below one second must not change which bucket the value falls into.
SELECT
    countIf(toStartOfMinute(t64) > t64) AS minute_too_late,
    countIf(toStartOfHour(t64) > t64) AS hour_too_late,
    countIf(toStartOfDay(t64) > t64) AS day_too_late,
    countIf(timeSlot(t64) > t64) AS time_slot_too_late,
    countIf(toStartOfMinute(t64) != toStartOfMinute(t0)) AS minute_moved,
    countIf(toStartOfFiveMinutes(t64) != toStartOfFiveMinutes(t0)) AS five_minutes_moved,
    countIf(toStartOfTenMinutes(t64) != toStartOfTenMinutes(t0)) AS ten_minutes_moved,
    countIf(toStartOfFifteenMinutes(t64) != toStartOfFifteenMinutes(t0)) AS fifteen_minutes_moved,
    countIf(toStartOfHour(t64) != toStartOfHour(t0)) AS hour_moved,
    countIf(toStartOfDay(t64) != toStartOfDay(t0)) AS day_moved,
    countIf(timeSlot(t64) != timeSlot(t0)) AS time_slot_moved,
    countIf(toTimeWithFixedDate(t64) != toTimeWithFixedDate(t0)) AS time_with_fixed_date_moved
FROM (
    SELECT toDateTime64(-2208988800 + number * 1013, 0, 'UTC') AS t0,
           toDateTime64(-2208988800 + number * 1013, 3, 'UTC') + INTERVAL 999 MILLISECOND AS t64
    FROM numbers(100000));

-- `toDate` and `CAST` reach the same code through the conversion functions, where the result type cannot hold
-- a pre-1970 date, so the outcome depends on `date_time_overflow_behavior`. Whatever it is, a sub-second value
-- must agree with the same instant at scale 0 rather than being silently moved into 1970.
SELECT
    toDate(toDateTime64('1969-12-31 23:59:59.500', 3, 'UTC')) = toDate(toDateTime64('1969-12-31 23:59:59', 0, 'UTC')) AS ignore_agrees,
    CAST(toDateTime64('1969-12-31 23:59:59.500', 3, 'UTC') AS Date) = CAST(toDateTime64('1969-12-31 23:59:59', 0, 'UTC') AS Date) AS cast_agrees;

SELECT
    toDate(toDateTime64('1969-12-31 23:59:59.500', 3, 'UTC')) AS saturated,
    toDate(toDateTime64('1969-12-31 23:59:59.500', 3, 'UTC')) = toDate(toDateTime64('1969-12-31 23:59:59', 0, 'UTC')) AS saturate_agrees
SETTINGS date_time_overflow_behavior = 'saturate';
