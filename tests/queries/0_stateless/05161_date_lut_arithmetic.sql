-- The day of week is computed from the day number instead of read from the lookup table. Check it against
-- the day number over the whole table (146097 days). 1970-01-01 was a Thursday, so a Monday is 3 modulo 7.
SELECT count() AS days, countIf(toDayOfWeek(d) != ((toInt64(d) + 3) % 7 + 7) % 7 + 1) AS wrong
FROM (SELECT toDate32('1900-01-01') + number AS d FROM numbers(146097));

-- The same outside the table, where the day of week comes from the escape path, over the whole calendar
-- (0000-01-01 to 9999-12-31). The stride is coprime with 7, so all seven days are covered on both sides.
SELECT count() AS days, countIf(toDayOfWeek(t, 0, 'UTC') != ((intDiv(toInt64(t), 86400) + 3) % 7 + 7) % 7 + 1) AS wrong
FROM (SELECT toDateTime64(-62167219200 + number * 97 * 86400, 0, 'UTC') AS t FROM numbers(37654));

-- The boundaries: the first and the last day of the calendar, of the table, and of the epoch.
SELECT toString(t, 'UTC') AS day, toDayOfWeek(t, 0, 'UTC') AS day_of_week
FROM (SELECT toDateTime64(arrayJoin([-62167219200, -2209075200, -2208988800, 0, 10413705600, 10413792000, 253402214400]), 0, 'UTC') AS t)
ORDER BY t;

-- `toRelativeWeekNum` counts weeks from the Monday of the week, so it also depends only on the day number.
-- It returns a `UInt16`, so anything before the epoch week comes back as zero.
SELECT count() AS days, countIf(toRelativeWeekNum(d) != greatest(intDiv(toInt64(d) + 7 - ((toInt64(d) + 3) % 7 + 7) % 7, 7), 0)) AS wrong
FROM (SELECT toDate32('1900-01-01') + number AS d FROM numbers(146097));

-- The other week functions build on the day of week, so they must agree with it: a week starts on a Monday,
-- ends on a Sunday, is seven days long, contains its own day, and has one ISO week number. They return a
-- `Date` here, so the sweep stays in that range.
SELECT
    count() AS days,
    countIf(toDayOfWeek(toMonday(d)) != 1) AS monday_is_not_monday,
    countIf(toDayOfWeek(toLastDayOfWeek(d, 1)) != 7) AS last_day_is_not_sunday,
    countIf(NOT (toMonday(d) <= d AND d <= toLastDayOfWeek(d, 1))) AS day_outside_its_own_week,
    countIf(toLastDayOfWeek(d, 1) - toMonday(d) != 6) AS week_is_not_seven_days,
    countIf(toStartOfWeek(d, 1) != toMonday(d)) AS start_of_week_is_not_monday,
    countIf(toISOWeek(d) != toISOWeek(toMonday(d))) AS iso_week_differs_within_a_week,
    countIf(toISOWeek(d) NOT BETWEEN 1 AND 53) AS iso_week_out_of_range
FROM (SELECT toDate('1970-01-05') + number AS d FROM numbers(65000));

-- The same functions outside the table. A `Date32` result is not clamped, so each one can be checked against
-- the day number over the whole calendar. The first and the last week are left out: a week reaching past
-- 0000-01-01 or 9999-12-31 saturates to the boundary day. `toRelativeWeekNum` is still a `UInt16`, hence the clamp.
SELECT
    count() AS days,
    countIf(toDayOfWeek(d) != days_since_monday + 1) AS wrong_day_of_week,
    countIf(toInt64(toMonday(d)) != toInt64(d) - days_since_monday) AS wrong_monday,
    countIf(toStartOfWeek(d, 1) != toMonday(d)) AS start_of_week_is_not_monday,
    countIf(toInt64(toLastDayOfWeek(d, 1)) != toInt64(d) + 6 - days_since_monday) AS wrong_last_day_of_week,
    countIf(toRelativeWeekNum(d) != least(greatest(intDiv(toInt64(d) + 7 - days_since_monday, 7), 0), 65535)) AS wrong_relative_week_num,
    countIf(toISOWeek(d) != toISOWeek(toMonday(d))) AS iso_week_differs_within_a_week,
    countIf(toISOWeek(d) NOT BETWEEN 1 AND 53) AS iso_week_out_of_range
FROM (SELECT toDate32('0000-01-10') + number * 13 AS d, ((toInt64(d) + 3) % 7 + 7) % 7 AS days_since_monday FROM numbers(280955))
SETTINGS enable_extended_results_for_datetime_functions = 1;

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

-- Sub-hour offset changes in the middle of a day: `Australia/Lord_Howe` moves by 30 minutes twice a year,
-- and `Asia/Kathmandu` went from +05:30 to +05:45 in 1986.
CREATE TEMPORARY TABLE with_dst AS
    SELECT toDateTime64(500000000 + number * 997, 0, 'UTC') AS t FROM numbers(100000);

SELECT 'Australia/Lord_Howe',
    countIf(toHour(t, 'Australia/Lord_Howe') != toUInt8(substring(toString(t, 'Australia/Lord_Howe'), 12, 2))) AS wrong_hour,
    countIf(toMinute(t, 'Australia/Lord_Howe') != toUInt8(substring(toString(t, 'Australia/Lord_Howe'), 15, 2))) AS wrong_minute,
    countIf(toSecond(t, 'Australia/Lord_Howe') != toUInt8(substring(toString(t, 'Australia/Lord_Howe'), 18, 2))) AS wrong_second
FROM with_dst;

SELECT 'Asia/Kathmandu',
    countIf(toHour(t, 'Asia/Kathmandu') != toUInt8(substring(toString(t, 'Asia/Kathmandu'), 12, 2))) AS wrong_hour,
    countIf(toMinute(t, 'Asia/Kathmandu') != toUInt8(substring(toString(t, 'Asia/Kathmandu'), 15, 2))) AS wrong_minute,
    countIf(toSecond(t, 'Asia/Kathmandu') != toUInt8(substring(toString(t, 'Asia/Kathmandu'), 18, 2))) AS wrong_second
FROM with_dst;

-- The `Time` type splits a duration into hours, minutes and seconds over its whole range, up to three digits of hours.
SELECT count() AS values, countIf(toString(CAST(number AS Time)) != concat(
    if(intDiv(number, 3600) < 10, '0', ''), toString(intDiv(number, 3600)), ':',
    if(intDiv(number, 60) % 60 < 10, '0', ''), toString(intDiv(number, 60) % 60), ':',
    if(number % 60 < 10, '0', ''), toString(number % 60))) AS wrong
FROM numbers(3600000);
