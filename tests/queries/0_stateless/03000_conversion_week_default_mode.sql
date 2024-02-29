SELECT
    toDateTime('2024-01-02 00:00:00', 'UTC') dt,
    toStartOfWeek(dt) w,
    toStartOfInterval(dt, toIntervalWeek(1)) w_1,
    toStartOfInterval(dt, toIntervalWeek(2)) w_2,
    toBool(w - w_1 = 0) b_1,
    toBool(w - w_2 = 7) b_2
;

SELECT
    toDateTime('2024-01-02 00:00:00', 'UTC') dt,
    toStartOfWeek(dt) w,
    toStartOfInterval(dt, toIntervalWeek(1)) w_1,
    toStartOfInterval(dt, toIntervalWeek(2)) w_2,
    toBool(w - w_1 = 0) b_1,
    toBool(w - w_2 = 7) b_2
SETTINGS default_mode_week_functions = 'monday';

SELECT
    toDateTime('2024-01-02 00:00:00', 'UTC') dt,
    toStartOfWeek(dt) w,
    toStartOfInterval(dt, toIntervalWeek(1)) w_1,
    toStartOfInterval(dt, toIntervalWeek(2)) w_2,
    toBool(w - w_1 = 0) b_1,
    toBool(w - w_2 = 7) b_2
SETTINGS default_mode_week_functions = 'sunday';
