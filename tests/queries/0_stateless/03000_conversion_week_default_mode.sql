SELECT
    toDateTime('2024-01-02 00:00:00', 'UTC') dt,
    toStartOfWeek(dt) w, -- Sunday, Dec 31
    toStartOfInterval(dt, toIntervalWeek(1)) w_1, -- Monday, Jan 01
    toStartOfInterval(dt, toIntervalWeek(2)) w_2, -- Monday, Dec 25
    toBool(w - w_1 = 0) b_1,
    toBool(w - w_2 = 7) b_2,
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
    toDateTime('2024-01-02 00:00:00', 'UTC') dt,
    toStartOfWeek(dt) w, -- Sunday, Dec 31
    toStartOfInterval(dt, toIntervalWeek(1)) w_1, -- Monday, Jan 01
    toStartOfInterval(dt, toIntervalWeek(2)) w_2, -- Monday, Dec 25
    toBool(w - w_1 = 0) b_1,
    toBool(w - w_2 = 7) b_2,
    toDateTime('2023-01-22 00:00:00', 'UTC') sunday,
    toDateTime('2023-01-23 00:00:00', 'UTC') monday,
    toDateTime('2023-01-24 00:00:00', 'UTC') tuesday,
    dateDiff('week', monday, tuesday),
    dateDiff('week', sunday, monday),
    age('week', monday, tuesday),
    age('week', sunday, monday),
    age('week', sunday, monday + toIntervalDay(10))
SETTINGS default_mode_week_functions = 'monday';

SELECT
    toDateTime('2024-01-02 00:00:00', 'UTC') dt,
    toStartOfWeek(dt) w, -- Sunday, Dec 31
    toStartOfInterval(dt, toIntervalWeek(1)) w_1, -- Sunday, Dec 31
    toStartOfInterval(dt, toIntervalWeek(2)) w_2, -- Sunday, Dec 24
    toBool(w - w_1 = 0) b_1,
    toBool(w - w_2 = 7) b_2,
    toDateTime('2023-01-22 00:00:00', 'UTC') sunday,
    toDateTime('2023-01-23 00:00:00', 'UTC') monday,
    toDateTime('2023-01-24 00:00:00', 'UTC') tuesday,
    dateDiff('week', monday, tuesday),
    dateDiff('week', sunday, monday),
    age('week', monday, tuesday),
    age('week', sunday, monday),
    age('week', sunday, monday + toIntervalDay(10))
SETTINGS default_mode_week_functions = 'sunday';
