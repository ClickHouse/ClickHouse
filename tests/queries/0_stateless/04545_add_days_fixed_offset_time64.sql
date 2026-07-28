SET enable_time_time64_type = 1;

SELECT 'fixed offset';
SELECT
    sum(addDays(t, delta) != addHours(t, delta * 24)),
    sum(addWeeks(t, delta) != addHours(t, delta * 168)),
    sum(subtractDays(t, delta) != subtractHours(t, delta * 24)),
    sum(subtractWeeks(t, delta) != subtractHours(t, delta * 168))
FROM
(
    SELECT
        materialize(toTime64('12:34:56.789', 3)) AS t,
        toInt64(number) - 2 AS delta
    FROM numbers(5)
)
SETTINGS session_timezone = 'UTC';

SELECT 'non-fixed offset';
-- Santiago started DST on 1969-11-23, within the `Time64` range around the Unix epoch.
-- The calendar shifts below are therefore 23 and 167 hours, respectively.
SELECT
    toInt64(addDays(start_time, delta)),
    toInt64(addWeeks(start_time, delta)),
    toInt64(subtractDays(day_end, delta)),
    toInt64(subtractWeeks(week_end, delta))
FROM
(
    SELECT
        materialize(toTime64(-3398400, 0)) AS start_time,
        materialize(toTime64(-3315600, 0)) AS day_end,
        materialize(toTime64(-2797200, 0)) AS week_end,
        toInt64(number + 1) AS delta
    FROM numbers(1)
)
SETTINGS session_timezone = 'America/Santiago';
