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
