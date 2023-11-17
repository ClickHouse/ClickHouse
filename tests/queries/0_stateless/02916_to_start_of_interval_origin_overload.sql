set session_timezone = 'UTC';
SELECT toStartOfInterval(number % 2 == 0 ? toDateTime64('2023-03-01 15:55:00', 2) : toDateTime64('2023-02-01 15:55:00', 2), toIntervalMinute(1), toDateTime64('2023-01-01 13:55:00', 2), 'Europe/Amsterdam') from numbers(5);
SELECT toStartOfInterval(number % 2 == 0 ? toDateTime('2023-03-01 15:55:00') : toDateTime('2023-02-01 15:55:00'), toIntervalMinute(1), toDateTime('2023-01-01 13:55:00'), 'Europe/Amsterdam') from numbers(5);
SELECT toStartOfInterval(toDateTime('2023-01-02 14:45:50'), toIntervalHour(1), toDateTime('2023-01-02 14:44:30'), 'Europe/Amsterdam');
SELECT toStartOfInterval(toDateTime64('2023-01-02 14:45:50', 2), toIntervalHour(1), toDateTime64('2023-01-02 14:44:30', 2), 'Europe/Amsterdam');
SELECT toStartOfInterval(toDateTime64('2023-01-02 14:45:50', 2), toIntervalMinute(1), toDateTime64('2023-01-02 14:44:30', 2));
SELECT toStartOfInterval(toDateTime('2023-01-02 14:45:50'), toIntervalMinute(1), toDateTime('2023-01-02 14:44:30'));
SELECT toStartOfInterval(toDate('2023-01-02 14:45:50'), toIntervalWeek(1), toDate('2023-01-02 14:44:30'));
SELECT toStartOfInterval(toDateTime('2023-01-02 14:45:50'), toIntervalMinute(1), toDateTime64('2023-01-02 14:44:30', 2)); -- { serverError 43 }
SELECT toStartOfInterval(toDateTime64('2023-01-02 14:45:50', 2), toIntervalMinute(1), toDate('2023-01-02 14:44:30')); -- { serverError 43 }
SELECT toStartOfInterval(toDateTime('2023-01-02 14:42:50'), toIntervalMinute(1), toDateTime('2023-01-02 14:44:30')); -- { serverError 36 }
SELECT toStartOfInterval(toDateTime('2023-01-02 14:45:50'), toIntervalMinute(1), number % 2 == 0 ? toDateTime('2023-02-01 15:55:00') : toDateTime('2023-01-01 15:55:00'), 'Europe/Amsterdam') from numbers(1); -- { serverError 44 }
