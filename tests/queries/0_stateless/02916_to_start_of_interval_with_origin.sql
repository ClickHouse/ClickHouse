SET session_timezone = 'UTC'; -- block time zone randomization in CI

SELECT '-- Negative tests';

-- the first argument must be a Date, DateTime or DateTime64
SELECT toStartOfInterval(toDate32('2023-01-02 10:11:12'), toIntervalHour(5)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT toStartOfInterval(toDate32('2023-01-02 10:11:12'), toIntervalHour(5), toDate32('2023-01-01 10:11:12')); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- the second argument must be a const time interval interval
SELECT toStartOfInterval(toDate('2023-01-02'), 3); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT toStartOfInterval(toDate('2023-01-02'), materialize(toIntervalWeek(1))); -- { serverError ILLEGAL_COLUMN }

-- if the value is a Date, no timezone argument must be given
SELECT toStartOfInterval(toDate('2023-01-02'), toIntervalWeek(5), 'UTC'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT toStartOfInterval(toDate('2023-01-02'), toIntervalWeek(5), toDate('2023-01-01'), 'UTC'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- time and origin arguments must have the same type
SELECT toStartOfInterval(toDate('2023-01-02'), toIntervalSecond(5), toDateTime('2023-01-01 10:11:12')); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT toStartOfInterval(toDateTime('2023-01-02 10:11:12'), toIntervalMinute(1), toDateTime64('2023-01-01 10:11:12', 3)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT toStartOfInterval(toDateTime64('2023-01-02 10:11:12', 3), toIntervalMinute(1), toDate('2023-01-01')); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- time and origin arguments must have the same scale if they are DateTime64
SELECT toStartOfInterval(toDateTime64('2023-01-02 10:11:12', 3), toIntervalMinute(1), toDateTime64('2023-01-01', 6)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- the origin must be constant
SELECT toStartOfInterval(toDateTime('2023-01-02 10:45:50'), toIntervalHour(1), materialize(toDateTime('2023-01-02 10:11:12'))); -- { serverError ILLEGAL_COLUMN }
SELECT toStartOfInterval(toDateTime('2023-01-02 10:45:50'), toIntervalHour(1), materialize(toDateTime('2023-01-02 10:11:12')), 'UTC'); -- { serverError ILLEGAL_COLUMN }

-- the origin must be before the time TODO
-- SELECT toStartOfInterval(toDateTime('2023-01-02 14:42:50'), toIntervalMinute(1), toDateTime('2023-01-02 14:11:12')); -- { serverError BAD_ARGUMENTS }

-- with 4 arguments, the 3rd one must be a origin Date, DateTime or DateTime64
SELECT toStartOfInterval(toDateTime('2023-01-02 10:45:50'), toIntervalYear(1), 5, 'UTC'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT toStartOfInterval(toDateTime('2023-01-02 10:45:50'), toIntervalYear(1), 'UTC', 'UTC'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- must have 4 or fewer arguments
SELECT toStartOfInterval(toDateTime('2023-01-02 10:45:50'), toIntervalYear(1), toDateTime('2020-01-02 10:11:12'), 'UTC', 5); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

SELECT '-- time argument is of type Date';
SELECT toStartOfInterval(toDate('2023-10-09'), toIntervalYear(1), toDate('2022-09-22'));
SELECT toStartOfInterval(toDate('2023-10-09'), toIntervalQuarter(1), toDate('2022-09-22'));
SELECT toStartOfInterval(toDate('2023-10-09'), toIntervalMonth(1), toDate('2022-09-22'));
-- SELECT toStartOfInterval(toDate('2023-10-09'), toIntervalWeek(1), toDate('2023-10-04')); -- produces BS
-- SELECT toStartOfInterval(toDate('2023-10-09'), toIntervalDay(1), toDate('2023-10-04')); -- produces even worse BS
-- Intervals with higher precision are rejected
SELECT toStartOfInterval(toDate('2023-10-09'), toIntervalMinute(1), toDate('2023-10-02')); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT toStartOfInterval(toDate('2023-10-09'), toIntervalMicrosecond(1), toDate('2023-10-02')); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT '-- time argument is of type DateTime';
-- intervals year, quarter, mont, week cut off the time (by design, but still unfortunately TBD)
SELECT toStartOfInterval(toDateTime('2023-10-09 10:11:12'), toIntervalYear(1), toDateTime('2022-09-22 02:03:04'));
SELECT toStartOfInterval(toDateTime('2023-10-09 10:11:12'), toIntervalQuarter(1), toDateTime('2022-09-22 02:03:04'));
SELECT toStartOfInterval(toDateTime('2023-10-09 10:11:12'), toIntervalMonth(1), toDateTime('2022-09-22 02:03:04'));
-- SELECT toStartOfInterval(toDateTime('2023-10-09 10:11:12'), toIntervalWeek(1), toDateTime('2022-09-22 02:03:04'));
-- SELECT toStartOfInterval(toDateTime('2023-10-09 10:11:12'), toIntervalDay(1), toDateTime('2022-09-22 02:03:04'));
-- SELECT toStartOfInterval(toDateTime('2023-10-09 10:11:12'), toIntervalHour(1), toDateTime('2023-10-02 02:03:04'));
-- SELECT toStartOfInterval(toDateTime('2023-10-09 10:11:12'), toIntervalMinute(1), toDateTime('2023-10-02 02:03:04'));
-- SELECT toStartOfInterval(toDateTime('2023-10-09 10:11:12'), toIntervalSecond(1), toDateTime('2023-10-02 02:03:04'));
-- SELECT toStartOfInterval(toDateTime('2023-10-09 10:11:12'), toIntervalMicrosecond(1), toDateTime('2023-10-02 02:03:04')); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- To be continued here ...
