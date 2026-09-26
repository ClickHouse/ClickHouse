-- With `enable_extended_results_for_datetime_functions = 1`, `toStartOfInterval` must return the extended types
-- `Date32` and `DateTime64` only for `Date32` and `DateTime64` arguments, the same as the `toStartOfX` functions.
-- `Date` and `DateTime` arguments keep the `Date` and `DateTime` result types.
-- https://github.com/ClickHouse/ClickHouse/issues/121771

-- The setting has an effect only when the result type depends on the interval unit.
SET to_start_of_interval_preserves_argument_type = 0;

SET enable_extended_results_for_datetime_functions = 1;

SELECT 'DateTime argument';
SELECT
    toTypeName(toStartOfInterval(toDateTime('2023-01-15 14:30:00', 'UTC'), INTERVAL 1 SECOND)),
    toTypeName(toStartOfInterval(toDateTime('2023-01-15 14:30:00', 'UTC'), INTERVAL 1 MINUTE)),
    toTypeName(toStartOfInterval(toDateTime('2023-01-15 14:30:00', 'UTC'), INTERVAL 1 HOUR)),
    toTypeName(toStartOfInterval(toDateTime('2023-01-15 14:30:00', 'UTC'), INTERVAL 1 DAY)),
    toTypeName(toStartOfInterval(toDateTime('2023-01-15 14:30:00', 'UTC'), INTERVAL 1 WEEK)),
    toTypeName(toStartOfInterval(toDateTime('2023-01-15 14:30:00', 'UTC'), INTERVAL 1 MONTH)),
    toTypeName(toStartOfInterval(toDateTime('2023-01-15 14:30:00', 'UTC'), INTERVAL 1 QUARTER)),
    toTypeName(toStartOfInterval(toDateTime('2023-01-15 14:30:00', 'UTC'), INTERVAL 1 YEAR))
FORMAT TSV;

SELECT 'Date argument';
SELECT
    toTypeName(toStartOfInterval(toDate('2023-01-15'), INTERVAL 1 DAY)),
    toTypeName(toStartOfInterval(toDate('2023-01-15'), INTERVAL 1 WEEK)),
    toTypeName(toStartOfInterval(toDate('2023-01-15'), INTERVAL 1 MONTH)),
    toTypeName(toStartOfInterval(toDate('2023-01-15'), INTERVAL 1 QUARTER)),
    toTypeName(toStartOfInterval(toDate('2023-01-15'), INTERVAL 1 YEAR))
FORMAT TSV;

SELECT 'DateTime64 argument';
SELECT
    toTypeName(toStartOfInterval(toDateTime64('2023-01-15 14:30:00', 3, 'UTC'), INTERVAL 1 HOUR)),
    toTypeName(toStartOfInterval(toDateTime64('2023-01-15 14:30:00', 3, 'UTC'), INTERVAL 1 DAY)),
    toTypeName(toStartOfInterval(toDateTime64('2023-01-15 14:30:00', 3, 'UTC'), INTERVAL 1 MONTH))
FORMAT TSV;

SELECT 'Date32 argument';
SELECT
    toTypeName(toStartOfInterval(toDate32('2023-01-15'), INTERVAL 1 DAY)),
    toTypeName(toStartOfInterval(toDate32('2023-01-15'), INTERVAL 1 WEEK)),
    toTypeName(toStartOfInterval(toDate32('2023-01-15'), INTERVAL 1 MONTH))
FORMAT TSV;

SELECT 'Same type and value as toStartOfX for Date and DateTime arguments';
WITH toDateTime('2023-01-15 14:30:45', 'UTC') AS dt, toDate('2023-01-15') AS d
SELECT
    toStartOfInterval(dt, INTERVAL 1 HOUR) = toStartOfHour(dt) AND toTypeName(toStartOfInterval(dt, INTERVAL 1 HOUR)) = toTypeName(toStartOfHour(dt)),
    toStartOfInterval(dt, INTERVAL 1 DAY) = toStartOfDay(dt) AND toTypeName(toStartOfInterval(dt, INTERVAL 1 DAY)) = toTypeName(toStartOfDay(dt)),
    toStartOfInterval(d, INTERVAL 1 MONTH) = toStartOfMonth(d) AND toTypeName(toStartOfInterval(d, INTERVAL 1 MONTH)) = toTypeName(toStartOfMonth(d)),
    toStartOfInterval(d, INTERVAL 1 YEAR) = toStartOfYear(d) AND toTypeName(toStartOfInterval(d, INTERVAL 1 YEAR)) = toTypeName(toStartOfYear(d))
FORMAT TSV;

SELECT 'Extended arguments still get extended results';
SELECT
    toStartOfInterval(toDateTime64('1969-12-31 12:00:00.123', 3, 'UTC'), INTERVAL 1 DAY),
    toStartOfInterval(toDate32('1969-12-31'), INTERVAL 1 MONTH)
FORMAT TSV;

SELECT 'Setting disabled';
SET enable_extended_results_for_datetime_functions = 0;
SELECT
    toTypeName(toStartOfInterval(toDateTime64('2023-01-15 14:30:00', 3, 'UTC'), INTERVAL 1 HOUR)),
    toTypeName(toStartOfInterval(toDate32('2023-01-15'), INTERVAL 1 MONTH))
FORMAT TSV;
