set session_timezone = 'UTC';

SELECT '-- Negative tests';

-- time and origin arguments must have the same type
SELECT toStartOfInterval(toDate('2023-01-02 14:45:50'), toIntervalSecond(5), toDate32('2023-01-02 14:44:30')); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT toStartOfInterval(toDate('2023-01-02 14:45:50'), toIntervalMillisecond(12), toDateTime('2023-01-02 14:44:30')); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT toStartOfInterval(toDate32('2023-01-02 14:45:50'), toIntervalHour(5), toDate('2023-01-02 14:44:30')); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT toStartOfInterval(toDateTime('2023-01-02 14:45:50'), toIntervalMinute(1), toDateTime64('2023-01-02 14:44:30', 2)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT toStartOfInterval(toDateTime64('2023-01-02 14:45:50', 2), toIntervalMinute(1), toDate('2023-01-02 14:44:30')); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- the origin must be before the time
SELECT toStartOfInterval(toDateTime('2023-01-02 14:42:50'), toIntervalMinute(1), toDateTime('2023-01-02 14:44:30')); -- { serverError BAD_ARGUMENTS }

-- the origin must be constant
SELECT toStartOfInterval(toDateTime('2023-01-02 14:45:50'), toIntervalMinute(1), number % 2 == 0 ? toDateTime('2023-02-01 15:55:00') : toDateTime('2023-01-01 15:55:00')) from numbers(1); -- { serverError ILLEGAL_COLUMN }
SELECT toStartOfInterval(toDateTime('2023-01-02 14:45:50'), toIntervalHour(1), materialize(toDateTime('2023-01-02 14:44:30')), 'Europe/Amsterdam'); -- { serverError ILLEGAL_COLUMN }

-- with 4 arguments, the 3rd one must not be a string or an integer
SELECT toStartOfInterval(toDateTime('2023-01-02 14:45:50'), toIntervalYear(1), 'Europe/Amsterdam', 'Europe/Amsterdam'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT toStartOfInterval(toDateTime('2023-01-02 14:45:50'), toIntervalYear(1), 5, 'Europe/Amsterdam'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- too many arguments
SELECT toStartOfInterval(toDateTime('2023-01-02 14:45:50'), toIntervalYear(1), toDateTime('2020-01-02 14:44:30'), 'Europe/Amsterdam', 5); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

SELECT 'Time and origin as Date';
SELECT toStartOfInterval(toDate('2023-10-09'), toIntervalYear(1), toDate('2022-02-01'));
SELECT toStartOfInterval(toDate('2023-10-09'), toIntervalQuarter(1), toDate('2022-02-01'));
SELECT toStartOfInterval(toDate('2023-10-09'), toIntervalMonth(1), toDate('2023-09-08'));
SELECT toStartOfInterval(toDate('2023-10-09'), toIntervalWeek(1), toDate('2023-10-01'));
SELECT toStartOfInterval(toDate('2023-10-09'), toIntervalDay(1), toDate('2023-10-08'));
SELECT toStartOfInterval(toDate('2023-10-09'), toIntervalHour(1), toDate('2023-10-09')); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT toStartOfInterval(toDate('2023-10-09'), toIntervalMinute(1), toDate('2023-10-09')); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT toStartOfInterval(toDate('2023-10-09'), toIntervalSecond(1), toDate('2023-10-09')); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT toStartOfInterval(toDate('2023-10-09'), toIntervalMillisecond(1), toDate('2023-10-09')); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT toStartOfInterval(toDate('2023-10-09'), toIntervalMicrosecond(1), toDate('2023-10-09')); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT toStartOfInterval(toDate('2023-10-09'), toIntervalNanosecond(1), toDate('2023-10-09')); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT 'Time and origin as Date32';
SELECT toStartOfInterval(toDate32('2023-10-09'), toIntervalYear(1), toDate32('2022-02-01'));
SELECT toStartOfInterval(toDate32('2023-10-09'), toIntervalQuarter(1), toDate32('2022-02-01'));
SELECT toStartOfInterval(toDate32('2023-10-09'), toIntervalMonth(1), toDate32('2023-09-08'));
SELECT toStartOfInterval(toDate32('2023-10-09'), toIntervalWeek(1), toDate32('2023-10-01'));
SELECT toStartOfInterval(toDate32('2023-10-09'), toIntervalDay(1), toDate32('2023-10-08'));
SELECT toStartOfInterval(toDate32('2023-10-09'), toIntervalHour(1), toDate32('2023-10-09')); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT toStartOfInterval(toDate32('2023-10-09'), toIntervalMinute(1), toDate32('2023-10-09')); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT toStartOfInterval(toDate32('2023-10-09'), toIntervalSecond(1), toDate32('2023-10-09')); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT toStartOfInterval(toDate32('2023-10-09'), toIntervalMillisecond(1), toDate32('2023-10-09')); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT toStartOfInterval(toDate32('2023-10-09'), toIntervalMicrosecond(1), toDate32('2023-10-09')); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT toStartOfInterval(toDate32('2023-10-09'), toIntervalNanosecond(1), toDate32('2023-10-09')); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT 'Time and origin as DateTime';
SELECT toStartOfInterval(toDateTime('2023-10-09 10:11:12'), toIntervalYear(1), toDateTime('2022-02-01 09:08:07'));
SELECT toStartOfInterval(toDateTime('2023-10-09 10:11:12'), toIntervalQuarter(1), toDateTime('2022-02-01 09:08:07'));
SELECT toStartOfInterval(toDateTime('2023-10-09 10:11:12'), toIntervalMonth(1), toDateTime('2023-09-08 09:08:07'));
SELECT toStartOfInterval(toDateTime('2023-10-09 10:11:12'), toIntervalWeek(1), toDateTime('2023-10-01 09:08:07'));
SELECT toStartOfInterval(toDateTime('2023-10-09 10:11:12'), toIntervalDay(1), toDateTime('2023-10-08 09:08:07'));
SELECT toStartOfInterval(toDateTime('2023-10-09 10:11:12'), toIntervalHour(1), toDateTime('2023-10-09 09:10:07'));
SELECT toStartOfInterval(toDateTime('2023-10-09 10:11:12'), toIntervalMinute(1), toDateTime('2023-10-09 09:10:07'));
SELECT toStartOfInterval(toDateTime('2023-10-09 10:11:12'), toIntervalSecond(1), toDateTime('2023-10-09 09:10:07'));
SELECT toStartOfInterval(toDateTime('2023-10-09 10:11:12'), toIntervalMillisecond(1), toDateTime('2023-10-09 10:11:12')); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT toStartOfInterval(toDateTime('2023-10-09 10:11:12'), toIntervalMicrosecond(1), toDateTime('2023-10-09 10:11:12')); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT toStartOfInterval(toDateTime('2023-10-09 10:11:12'), toIntervalNanosecond(1), toDateTime('2023-10-09 10:11:12')); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT 'Time and origin as DateTime64(9)';
SELECT toStartOfInterval(toDateTime64('2023-10-09 10:11:12.987654321', 9), toIntervalYear(1), toDateTime64('2022-02-01 09:08:07.123456789', 9));
SELECT toStartOfInterval(toDateTime64('2023-10-09 10:11:12.987654321', 9), toIntervalQuarter(1), toDateTime64('2022-02-01 09:08:07.123456789', 9));
SELECT toStartOfInterval(toDateTime64('2023-10-09 10:11:12.987654321', 9), toIntervalMonth(1), toDateTime64('2023-09-10 09:08:07.123456789', 9));
SELECT toStartOfInterval(toDateTime64('2023-10-09 10:11:12.987654321', 9), toIntervalWeek(1), toDateTime64('2023-10-01 09:08:07.123456789', 9));
SELECT toStartOfInterval(toDateTime64('2023-10-09 10:11:12.987654321', 9), toIntervalDay(1), toDateTime64('2023-10-08 09:08:07.123456789', 9));
SELECT toStartOfInterval(toDateTime64('2023-10-09 10:11:12.987654321', 9), toIntervalHour(1), toDateTime64('2023-10-09 09:10:07.123456789', 9));
SELECT toStartOfInterval(toDateTime64('2023-10-09 10:11:12.987654321', 9), toIntervalMinute(1), toDateTime64('2023-10-09 09:10:11.123456789', 9));
SELECT toStartOfInterval(toDateTime64('2023-10-09 10:11:12.987654321', 9), toIntervalSecond(1), toDateTime64('2023-10-09 10:11:10.123456789', 9));
SELECT toStartOfInterval(toDateTime64('2023-10-09 10:11:12.987654321', 9), toIntervalMillisecond(1), toDateTime64('2023-10-09 10:11:12.123456789', 9));
SELECT toStartOfInterval(toDateTime64('2023-10-09 10:11:12.987654321', 9), toIntervalMicrosecond(1), toDateTime64('2023-10-09 10:11:12.123456789', 9));
SELECT toStartOfInterval(toDateTime64('2023-10-09 10:11:12.987654321', 9), toIntervalNanosecond(1), toDateTime64('2023-10-09 10:11:12.123456789', 9));

SELECT 'Time and origin as DateTime64(3)';
SELECT toStartOfInterval(toDateTime64('2023-10-09 10:11:12.987', 3), toIntervalYear(1), toDateTime64('2022-02-01 09:08:07.123', 3));
SELECT toStartOfInterval(toDateTime64('2023-10-09 10:11:12.987', 3), toIntervalQuarter(1), toDateTime64('2022-02-01 09:08:07.123', 3));
SELECT toStartOfInterval(toDateTime64('2023-10-09 10:11:12.987', 3), toIntervalMonth(1), toDateTime64('2023-09-08 09:08:07.123', 3));
SELECT toStartOfInterval(toDateTime64('2023-10-09 10:11:12.987', 3), toIntervalWeek(1), toDateTime64('2023-10-01 09:08:07.123', 3));
SELECT toStartOfInterval(toDateTime64('2023-10-09 10:11:12.987', 3), toIntervalDay(1), toDateTime64('2023-10-08 09:08:07.123', 3));
SELECT toStartOfInterval(toDateTime64('2023-10-09 10:11:12.987', 3), toIntervalHour(1), toDateTime64('2023-10-09 09:10:07.123', 3));
SELECT toStartOfInterval(toDateTime64('2023-10-09 10:11:12.987', 3), toIntervalMinute(1), toDateTime64('2023-10-09 10:10:11.123', 3));
SELECT toStartOfInterval(toDateTime64('2023-10-09 10:11:12.987', 3), toIntervalSecond(1), toDateTime64('2023-10-09 10:11:10.123', 3));
SELECT toStartOfInterval(toDateTime64('2023-10-09 10:11:12.987', 3), toIntervalMillisecond(1), toDateTime64('2023-10-09 10:11:12.123', 3));
SELECT toStartOfInterval(toDateTime64('2023-10-09 10:11:12.987', 3), toIntervalMicrosecond(1), toDateTime64('2023-10-09 10:11:12.123', 3));
SELECT toStartOfInterval(toDateTime64('2023-10-09 10:11:12.987', 3), toIntervalNanosecond(1), toDateTime64('2023-10-09 10:11:12.123', 3));

SELECT 'Non-const arguments';
SELECT toStartOfInterval(number % 2 == 0 ? toDateTime64('2023-03-01 15:55:00', 2) : toDateTime64('2023-02-01 15:55:00', 2), toIntervalMinute(1), toDateTime64('2023-01-01 13:55:00', 2), 'Europe/Amsterdam') from numbers(5);
SELECT toStartOfInterval(number % 2 == 0 ? toDateTime('2023-03-01 15:55:00') : toDateTime('2023-02-01 15:55:00'), toIntervalHour(1), toDateTime('2023-01-01 13:55:00'), 'Europe/Amsterdam') from numbers(5);
SELECT toStartOfInterval(materialize(toDateTime('2023-01-02 14:45:50')), toIntervalHour(1), toDateTime('2023-01-02 14:44:30'), 'Europe/Amsterdam');
SELECT toStartOfInterval(materialize(toDateTime64('2023-02-01 15:45:50', 2)), toIntervalHour(1), toDateTime64('2023-01-02 14:44:30', 2), 'Europe/Amsterdam');
