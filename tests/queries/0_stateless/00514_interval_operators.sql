SET session_timezone = 'Etc/UTC';

SELECT toDateTime('2017-10-30 08:18:19') + INTERVAL 1 DAY + INTERVAL 1 MONTH - INTERVAL 1 YEAR;
SELECT toDateTime('2017-10-30 08:18:19') + INTERVAL 1 HOUR + INTERVAL 1000 MINUTE + INTERVAL 10 SECOND;
SELECT toDateTime('2017-10-30 08:18:19') + INTERVAL 1 DAY + INTERVAL number MONTH FROM system.numbers LIMIT 20;
SELECT toDateTime('2016-02-29 01:02:03') + INTERVAL number YEAR, toDateTime('2016-02-29 01:02:03') + INTERVAL number MONTH FROM system.numbers LIMIT 16;
SELECT toDateTime('2016-02-29 01:02:03') - INTERVAL 1 QUARTER;

SELECT (toDateTime('2000-01-01 12:00:00') + INTERVAL 1234567 SECOND) x, toTypeName(x);
SELECT (toDateTime('2000-01-01 12:00:00') + INTERVAL 1234567 MILLISECOND) x, toTypeName(x);
SELECT (toDateTime('2000-01-01 12:00:00') + INTERVAL 1234567 MICROSECOND) x, toTypeName(x);
SELECT (toDateTime('2000-01-01 12:00:00') + INTERVAL 1234567 NANOSECOND) x, toTypeName(x);

SELECT (toDateTime('2000-01-01 12:00:00') - INTERVAL 1234567 SECOND) x, toTypeName(x);
SELECT (toDateTime('2000-01-01 12:00:00') - INTERVAL 1234567 MILLISECOND) x, toTypeName(x);
SELECT (toDateTime('2000-01-01 12:00:00') - INTERVAL 1234567 MICROSECOND) x, toTypeName(x);
SELECT (toDateTime('2000-01-01 12:00:00') - INTERVAL 1234567 NANOSECOND) x, toTypeName(x);

SELECT (toDateTime64('2000-01-01 12:00:00.678', 3) - INTERVAL 12345 MILLISECOND) x, toTypeName(x);
SELECT (toDateTime64('2000-01-01 12:00:00.67898', 5) - INTERVAL 12345 MILLISECOND) x, toTypeName(x);
SELECT (toDateTime64('2000-01-01 12:00:00.67', 2) - INTERVAL 12345 MILLISECOND) x, toTypeName(x);

select toDateTime64('3000-01-01 12:00:00.12345', 0) + interval 0 nanosecond; -- { serverError DECIMAL_OVERFLOW }
select toDateTime64('3000-01-01 12:00:00.12345', 0) + interval 0 microsecond;

-- Check that the error is thrown during typechecking, not execution.
select materialize(toDate('2000-01-01')) + interval 1 nanosecond from numbers(0); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
