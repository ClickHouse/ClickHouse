set session_timezone = 'UTC';

SELECT toStartOfInterval(toDateTime64('2023-01-02 14:45:50.917122341', 9), toIntervalNanosecond(10000), toDateTime64('2023-01-02 14:44:30.500600700', 9));
SELECT toStartOfInterval(toDateTime64('2023-01-02 14:45:50.917122', 6), toIntervalMicrosecond(10000), toDateTime64('2023-01-02 14:44:30.500600', 6));
SELECT toStartOfInterval(toDateTime64('2023-01-02 14:45:50.91', 3), toIntervalMillisecond(100), toDateTime64('2023-01-02 14:44:30.501', 3));
SELECT toStartOfInterval(toDateTime('2023-01-02 14:45:50'), toIntervalSecond(2), toDateTime('2023-01-02 14:44:30'));
SELECT toStartOfInterval(toDateTime('2023-01-02 14:45:50'), toIntervalMinute(1), toDateTime('2023-01-02 14:44:30'));
SELECT toStartOfInterval(toDateTime64('2023-01-02 14:45:50', 2), toIntervalHour(1), toDateTime64('2023-01-02 14:44:30', 2));
SELECT toStartOfInterval(toDateTime('2023-01-02 14:45:50'), toIntervalDay(1), toDateTime('2023-01-02 14:44:30'));
SELECT toStartOfInterval(toDateTime('2023-01-08 14:45:50'), toIntervalWeek(1), toDateTime('2023-01-02 14:44:30'));
SELECT toStartOfInterval(toDateTime('2023-03-03 14:45:50'), toIntervalMonth(1), toDateTime('2022-01-02 14:44:30'));
SELECT toStartOfInterval(toDateTime('2023-08-02 14:45:50'), toIntervalQuarter(1), toDateTime('2022-01-02 14:44:30'));
SELECT toStartOfInterval(toDateTime('2023-01-02 14:45:50'), toIntervalYear(1), toDateTime('2020-01-03 14:44:30'));

SELECT toStartOfInterval(toDateTime64('2023-01-02 14:45:50.917122341', 9), toIntervalNanosecond(10000), toDateTime64('2023-01-02 14:44:30.500600700', 9), 'Europe/Amsterdam');
SELECT toStartOfInterval(toDateTime64('2023-01-02 14:45:50.917122', 6), toIntervalMicrosecond(10000), toDateTime64('2023-01-02 14:44:30.500600', 6), 'Europe/Amsterdam');
SELECT toStartOfInterval(toDateTime64('2023-01-02 14:45:50.91', 3), toIntervalMillisecond(100), toDateTime64('2023-01-02 14:44:30.501', 3), 'Europe/Amsterdam');
SELECT toStartOfInterval(toDateTime('2023-01-02 14:45:50'), toIntervalSecond(2), toDateTime('2023-01-02 14:44:30'), 'Europe/Amsterdam');
SELECT toStartOfInterval(toDateTime('2023-01-02 14:45:50'), toIntervalMinute(1), toDateTime('2023-01-02 14:44:30'), 'Europe/Amsterdam');
SELECT toStartOfInterval(toDateTime64('2023-01-02 14:45:50', 2), toIntervalHour(1), toDateTime64('2023-01-02 14:44:30', 2), 'Europe/Amsterdam');
SELECT toStartOfInterval(toDateTime('2023-01-02 14:45:50'), toIntervalDay(1), toDateTime('2023-01-02 14:44:30'), 'Europe/Amsterdam');
SELECT toStartOfInterval(toDateTime('2023-01-08 14:45:50'), toIntervalWeek(1), toDateTime('2023-01-02 14:44:30'), 'Europe/Amsterdam');
SELECT toStartOfInterval(toDateTime('2023-03-03 14:45:50'), toIntervalMonth(1), toDateTime('2022-01-02 14:44:30'), 'Europe/Amsterdam');
SELECT toStartOfInterval(toDateTime('2023-08-02 14:45:50'), toIntervalQuarter(1), toDateTime('2022-01-02 14:44:30'), 'Europe/Amsterdam');
SELECT toStartOfInterval(toDateTime('2023-01-02 14:45:50'), toIntervalYear(1), toDateTime('2020-01-03 14:44:30'), 'Europe/Amsterdam');

SELECT toStartOfInterval(number % 2 == 0 ? toDateTime64('2023-03-01 15:55:00', 2) : toDateTime64('2023-02-01 15:55:00', 2), toIntervalMinute(1), toDateTime64('2023-01-01 13:55:00', 2), 'Europe/Amsterdam') from numbers(5);
SELECT toStartOfInterval(number % 2 == 0 ? toDateTime('2023-03-01 15:55:00') : toDateTime('2023-02-01 15:55:00'), toIntervalHour(1), toDateTime('2023-01-01 13:55:00'), 'Europe/Amsterdam') from numbers(5);
SELECT toStartOfInterval(materialize(toDateTime('2023-01-02 14:45:50')), toIntervalHour(1), toDateTime('2023-01-02 14:44:30'), 'Europe/Amsterdam');
SELECT toStartOfInterval(materialize(toDateTime64('2023-02-01 15:45:50', 2)), toIntervalHour(1), toDateTime64('2023-01-02 14:44:30', 2), 'Europe/Amsterdam');

SELECT toStartOfInterval(toDate('2023-01-02 14:45:50'), toIntervalSecond(5), toDate32('2023-01-02 14:44:30')); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT toStartOfInterval(toDate('2023-01-02 14:45:50'), toIntervalMillisecond(12), toDateTime('2023-01-02 14:44:30')); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT toStartOfInterval(toDate32('2023-01-02 14:45:50'), toIntervalHour(5), toDate('2023-01-02 14:44:30')); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT toStartOfInterval(toDateTime('2023-01-02 14:45:50'), toIntervalMinute(1), toDateTime64('2023-01-02 14:44:30', 2)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT toStartOfInterval(toDateTime64('2023-01-02 14:45:50', 2), toIntervalMinute(1), toDate('2023-01-02 14:44:30')); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT toStartOfInterval(toDateTime('2023-01-02 14:42:50'), toIntervalMinute(1), toDateTime('2023-01-02 14:44:30')); -- { serverError BAD_ARGUMENTS }
SELECT toStartOfInterval(toDateTime('2023-01-02 14:45:50'), toIntervalMinute(1), number % 2 == 0 ? toDateTime('2023-02-01 15:55:00') : toDateTime('2023-01-01 15:55:00'), 'Europe/Amsterdam') from numbers(1); -- { serverError ILLEGAL_COLUMN }
SELECT toStartOfInterval(toDateTime('2023-01-02 14:45:50'), toIntervalYear(1), 'Europe/Amsterdam', 'a'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT toStartOfInterval(toDateTime('2023-01-02 14:45:50'), toIntervalYear(1), toDateTime('2020-01-02 14:44:30'), 'Europe/Amsterdam', 5); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT toStartOfInterval(toDateTime('2023-01-02 14:45:50'), toIntervalYear(1), 5, 'Europe/Amsterdam'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT toStartOfInterval(toDateTime('2023-01-02 14:45:50'), toIntervalHour(1), materialize(toDateTime('2023-01-02 14:44:30')), 'Europe/Amsterdam'); -- { serverError ILLEGAL_COLUMN }
