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

-- An interval that does not divide a day, before the epoch. Both round the second interval from the epoch;
-- for the minute interval `Europe/Moscow` goes through the table and rounds from the start of the local day.
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

-- A minute interval is measured from the epoch and not from the start of the local day, so a zone whose
-- offset is a whole number of minutes that is not a multiple of the interval rounds to the same boundaries
-- on both sides of 1970: `Australia/Eucla` is +08:45 over the whole table, `Africa/Bamako` was -00:32 until
-- 1912. The scalar `toStartOf*Minutes` and the vectorized `toStartOfInterval` reach that through different
-- code, gated on the same offset property, so they must agree row by row.
SET enable_extended_results_for_datetime_functions = 1;

SELECT 'Australia/Eucla',
    countIf(toStartOfMinute(t) != toStartOfInterval(t, INTERVAL 1 MINUTE)) AS minute_disagrees,
    countIf(toStartOfFiveMinutes(t) != toStartOfInterval(t, INTERVAL 5 MINUTE)) AS five_minutes_disagree,
    countIf(toStartOfTenMinutes(t) != toStartOfInterval(t, INTERVAL 10 MINUTE)) AS ten_minutes_disagree,
    countIf(toStartOfFifteenMinutes(t) != toStartOfInterval(t, INTERVAL 15 MINUTE)) AS fifteen_minutes_disagree
FROM (SELECT toDateTime64(-2208900000 + number * 44351, 0, 'Australia/Eucla') AS t FROM numbers(100000));

SELECT 'Africa/Bamako',
    countIf(toStartOfMinute(t) != toStartOfInterval(t, INTERVAL 1 MINUTE)) AS minute_disagrees,
    countIf(toStartOfFiveMinutes(t) != toStartOfInterval(t, INTERVAL 5 MINUTE)) AS five_minutes_disagree,
    countIf(toStartOfTenMinutes(t) != toStartOfInterval(t, INTERVAL 10 MINUTE)) AS ten_minutes_disagree,
    countIf(toStartOfFifteenMinutes(t) != toStartOfInterval(t, INTERVAL 15 MINUTE)) AS fifteen_minutes_disagree
FROM (SELECT toDateTime64(-2208900000 + number * 44351, 0, 'Africa/Bamako') AS t FROM numbers(100000));

-- The same interval either side of the epoch in `Australia/Eucla`: a ten-minute interval lands on :45, :55
-- and :05 local, because the boundaries are the UTC ones and the offset is 45 minutes past the UTC hour.
SELECT toString(t, 'Australia/Eucla') AS local,
       toString(toStartOfTenMinutes(t), 'Australia/Eucla') AS start_of_ten_minutes,
       toString(toStartOfInterval(t, INTERVAL 10 MINUTE), 'Australia/Eucla') AS ten_minute_interval
FROM (SELECT arrayJoin([toDateTime64('1969-12-31 08:52:00', 0, 'Australia/Eucla'),
                        toDateTime64('1970-01-02 08:52:00', 0, 'Australia/Eucla')]) AS t)
ORDER BY t;

-- `Africa/Bamako` before 1912, where the offset is not a multiple of five minutes either.
SELECT toString(t, 'Africa/Bamako') AS local,
       toString(toStartOfFiveMinutes(t), 'Africa/Bamako') AS start_of_five_minutes,
       toString(toStartOfInterval(t, INTERVAL 5 MINUTE), 'Africa/Bamako') AS five_minute_interval
FROM (SELECT toDateTime64('1900-06-15 12:52:00', 0, 'Africa/Bamako') AS t);

-- And the zones the guard does exclude: a sub-minute component in the offset would put a modular result off
-- any local minute boundary, so these keep going through the table before the epoch.
SELECT toString(t, 'Europe/Amsterdam') AS local,
       toString(toStartOfTenMinutes(t), 'Europe/Amsterdam') AS start_of_ten_minutes,
       toString(toStartOfInterval(t, INTERVAL 10 MINUTE), 'Europe/Amsterdam') AS ten_minute_interval
FROM (SELECT toDateTime64('1930-06-15 12:52:00', 0, 'Europe/Amsterdam') AS t);

SELECT toString(t, 'Asia/Kolkata') AS local,
       toString(toStartOfTenMinutes(t), 'Asia/Kolkata') AS start_of_ten_minutes,
       toString(toStartOfInterval(t, INTERVAL 10 MINUTE), 'Asia/Kolkata') AS ten_minute_interval
FROM (SELECT toDateTime64('1902-06-15 12:52:00', 0, 'Asia/Kolkata') AS t);

-- A second interval needs no property of the offset: every UTC offset is a whole number of seconds, so a
-- modular result always lands on a local second boundary. `Australia/Eucla` (+08:45), `Pacific/Chatham`
-- (+12:45) and `Asia/Kolkata` (+05:30) bucket the same instants as `UTC`. The sweep spans 1899 to 2040.
SET enable_extended_results_for_datetime_functions = 1;

SELECT 'Australia/Eucla',
    countIf(toUnixTimestamp64Second(toStartOfInterval(t, INTERVAL 7 SECOND)) != ts - positiveModulo(ts, 7)) AS wrong_7s,
    countIf(toUnixTimestamp64Second(toStartOfInterval(t, INTERVAL 11 SECOND)) != ts - positiveModulo(ts, 11)) AS wrong_11s,
    countIf(toUnixTimestamp64Second(toStartOfInterval(t, INTERVAL 3601 SECOND)) != ts - positiveModulo(ts, 3601)) AS wrong_3601s
FROM (SELECT -2208900000 + number * 44351 AS ts, toDateTime64(ts, 0, 'Australia/Eucla') AS t FROM numbers(100000));

SELECT 'Pacific/Chatham',
    countIf(toUnixTimestamp64Second(toStartOfInterval(t, INTERVAL 7 SECOND)) != ts - positiveModulo(ts, 7)) AS wrong_7s,
    countIf(toUnixTimestamp64Second(toStartOfInterval(t, INTERVAL 11 SECOND)) != ts - positiveModulo(ts, 11)) AS wrong_11s,
    countIf(toUnixTimestamp64Second(toStartOfInterval(t, INTERVAL 3601 SECOND)) != ts - positiveModulo(ts, 3601)) AS wrong_3601s
FROM (SELECT -2208900000 + number * 44351 AS ts, toDateTime64(ts, 0, 'Pacific/Chatham') AS t FROM numbers(100000));

SELECT 'Asia/Kolkata',
    countIf(toUnixTimestamp64Second(toStartOfInterval(t, INTERVAL 7 SECOND)) != ts - positiveModulo(ts, 7)) AS wrong_7s,
    countIf(toUnixTimestamp64Second(toStartOfInterval(t, INTERVAL 11 SECOND)) != ts - positiveModulo(ts, 11)) AS wrong_11s,
    countIf(toUnixTimestamp64Second(toStartOfInterval(t, INTERVAL 3601 SECOND)) != ts - positiveModulo(ts, 3601)) AS wrong_3601s
FROM (SELECT -2208900000 + number * 44351 AS ts, toDateTime64(ts, 0, 'Asia/Kolkata') AS t FROM numbers(100000));

SELECT 'UTC',
    countIf(toUnixTimestamp64Second(toStartOfInterval(t, INTERVAL 7 SECOND)) != ts - positiveModulo(ts, 7)) AS wrong_7s,
    countIf(toUnixTimestamp64Second(toStartOfInterval(t, INTERVAL 11 SECOND)) != ts - positiveModulo(ts, 11)) AS wrong_11s,
    countIf(toUnixTimestamp64Second(toStartOfInterval(t, INTERVAL 3601 SECOND)) != ts - positiveModulo(ts, 3601)) AS wrong_3601s
FROM (SELECT -2208900000 + number * 44351 AS ts, toDateTime64(ts, 0, 'UTC') AS t FROM numbers(100000));

-- The start of the epoch itself: the bucket starts there, not a few seconds into it.
SELECT toString(t, 'Australia/Eucla') AS local,
       toString(toStartOfInterval(t, INTERVAL 11 SECOND), 'Australia/Eucla') AS eleven_second_interval
FROM (SELECT toDateTime64('1970-01-01 08:45:10', 0, 'Australia/Eucla') AS t);

SELECT toString(t, 'Pacific/Chatham') AS local,
       toString(toStartOfInterval(t, INTERVAL 11 SECOND), 'Pacific/Chatham') AS eleven_second_interval
FROM (SELECT toDateTime64('1970-01-01 12:45:03', 0, 'Pacific/Chatham') AS t);

-- The seam this leaves: a whole number of minutes is a minute interval and keeps measuring from the start of
-- the local day in `Europe/Amsterdam` (+00:19:32 until 1937), while one second more is measured from the epoch.
SELECT toString(t, 'Europe/Amsterdam') AS local,
       toString(toStartOfInterval(t, INTERVAL 60 SECOND), 'Europe/Amsterdam') AS sixty_second_interval,
       toString(toStartOfInterval(t, INTERVAL 61 SECOND), 'Europe/Amsterdam') AS sixty_one_second_interval
FROM (SELECT toDateTime64('1930-06-15 12:52:00', 0, 'Europe/Amsterdam') AS t);

-- The `origin` overload rounds the difference between the value and the origin, a duration and not a point in
-- time. Measuring that from the start of a local day put the start of the bucket before the origin.
WITH toDateTime64('2023-01-01 14:35:30', 0, 'Asia/Kolkata') AS origin
SELECT 'Asia/Kolkata',
    countIf(toStartOfInterval(t, INTERVAL 7 SECOND, origin) < origin) AS before_origin,
    countIf(toStartOfInterval(t, INTERVAL 7 SECOND, origin)
            != origin + intDiv(toUnixTimestamp64Second(t) - toUnixTimestamp64Second(origin), 7) * 7) AS wrong_7s
FROM (SELECT toDateTime64('2023-01-01 14:35:30', 0, 'Asia/Kolkata') + number AS t FROM numbers(100000));
