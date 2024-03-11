-- Testing setting behavior first_day_of_week.
-- It allows us to change `toStartOfInterval` and `date_diff` first day of week to Monday or Sunday

SELECT '-- toStartOfInterval';
SELECT
    toDateTime('2024-01-02 00:00:00', 'UTC') dt,
    toStartOfInterval(dt, INTERVAL 1 WEEK) w_1, -- Monday, Jan 01
    toStartOfInterval(dt, INTERVAL 2 WEEK) w_2 -- Monday, Dec 25
;

SELECT
    toDateTime('2024-01-02 00:00:00', 'UTC') dt,
    toStartOfInterval(dt, INTERVAL 1 WEEK) w_1, -- Monday, Jan 01
    toStartOfInterval(dt, INTERVAL 2 WEEK) w_2 -- Monday, Dec 25
SETTINGS first_day_of_week = 'Monday';

SELECT
    toDateTime('2024-01-02 00:00:00', 'UTC') dt,
    toStartOfInterval(dt, INTERVAL 1 WEEK) w_1, -- Sunday, Dec 31
    toStartOfInterval(dt, INTERVAL 2 WEEK) w_2 -- Sunday, Dec 24
SETTINGS first_day_of_week = 'Sunday';

SELECT '-- date_diff';
SELECT
    toDateTime('2023-01-22 00:00:00', 'UTC') sunday,
    toDateTime('2023-01-23 00:00:00', 'UTC') monday,
    toDateTime('2023-01-24 00:00:00', 'UTC') tuesday,
    dateDiff('week', monday, tuesday),
    dateDiff('week', sunday, monday),
    age('week', monday, tuesday),
    age('week', sunday, monday),
    age('week', sunday, monday + toIntervalDay(10))
;

SELECT
    toDateTime('2023-01-22 00:00:00', 'UTC') sunday,
    toDateTime('2023-01-23 00:00:00', 'UTC') monday,
    toDateTime('2023-01-24 00:00:00', 'UTC') tuesday,
    dateDiff('week', monday, tuesday),
    dateDiff('week', sunday, monday),
    age('week', monday, tuesday),
    age('week', sunday, monday),
    age('week', sunday, monday + toIntervalDay(10))
SETTINGS first_day_of_week = 'Monday';

SELECT
    toDateTime('2023-01-22 00:00:00', 'UTC') sunday,
    toDateTime('2023-01-23 00:00:00', 'UTC') monday,
    toDateTime('2023-01-24 00:00:00', 'UTC') tuesday,
    dateDiff('week', monday, tuesday),
    dateDiff('week', sunday, monday),
    age('week', monday, tuesday),
    age('week', sunday, monday),
    age('week', sunday, monday + toIntervalDay(10))
SETTINGS first_day_of_week = 'Sunday';
