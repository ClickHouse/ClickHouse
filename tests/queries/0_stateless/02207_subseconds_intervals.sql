SELECT 'test intervals';

SELECT '- test nanoseconds';
select toStartOfInterval(toDateTime64('1980-12-12 12:12:12.123456789', 9), INTERVAL 1 NANOSECOND); -- In normal range, source scale matches result
select toStartOfInterval(toDateTime64('1980-12-12 12:12:12.1234567', 7), INTERVAL 1 NANOSECOND); -- In normal range, source scale less than result

select toStartOfInterval(a, INTERVAL 1 NANOSECOND) from ( select toDateTime64('1980-12-12 12:12:12.123456789', 9) AS a ); -- Non-constant argument

select toStartOfInterval(toDateTime64('1930-12-12 12:12:12.123456789', 9), INTERVAL 1 NANOSECOND); -- Below normal range, source scale matches result
select toStartOfInterval(toDateTime64('1930-12-12 12:12:12.1234567', 7), INTERVAL 1 NANOSECOND); -- Below normal range, source scale less than result

select toStartOfInterval(toDateTime64('2220-12-12 12:12:12.123456789', 9), INTERVAL 1 NANOSECOND); -- Above normal range, source scale matches result
select toStartOfInterval(toDateTime64('2220-12-12 12:12:12.1234567', 7), INTERVAL 1 NANOSECOND); -- Above normal range, source scale less than result


SELECT '- test microseconds';
select toStartOfInterval(toDateTime64('1980-12-12 12:12:12.123456', 6), INTERVAL 1 MICROSECOND); -- In normal range, source scale matches result
select toStartOfInterval(toDateTime64('1980-12-12 12:12:12.1234', 4), INTERVAL 1 MICROSECOND); -- In normal range, source scale less than result
select toStartOfInterval(toDateTime64('1980-12-12 12:12:12.12345678', 8), INTERVAL 1 MICROSECOND); -- In normal range, source scale greater than result

select toStartOfInterval(a, INTERVAL 1 MICROSECOND) from ( select toDateTime64('1980-12-12 12:12:12.12345678', 8) AS a ); -- Non-constant argument

select toStartOfInterval(toDateTime64('1930-12-12 12:12:12.123456', 6), INTERVAL 1 MICROSECOND); -- Below normal range, source scale matches result
select toStartOfInterval(toDateTime64('1930-12-12 12:12:12.1234', 4), INTERVAL 1 MICROSECOND); -- Below normal range, source scale less than result
select toStartOfInterval(toDateTime64('1930-12-12 12:12:12.12345678', 8), INTERVAL 1 MICROSECOND); -- Below normal range, source scale greater than result


select toStartOfInterval(toDateTime64('2220-12-12 12:12:12.123456', 6), INTERVAL 1 MICROSECOND); -- Above normal range, source scale matches result
select toStartOfInterval(toDateTime64('2220-12-12 12:12:12.1234', 4), INTERVAL 1 MICROSECOND); -- Above normal range, source scale less than result
select toStartOfInterval(toDateTime64('2220-12-12 12:12:12.12345678', 8), INTERVAL 1 MICROSECOND); -- Above normal range, source scale greater than result


SELECT '- test milliseconds';
select toStartOfInterval(toDateTime64('1980-12-12 12:12:12.123', 3), INTERVAL 1 MILLISECOND); -- In normal range, source scale matches result
select toStartOfInterval(toDateTime64('1980-12-12 12:12:12.12', 2), INTERVAL 1 MILLISECOND); -- In normal range, source scale less than result
select toStartOfInterval(toDateTime64('1980-12-12 12:12:12.123456', 6), INTERVAL 1 MILLISECOND); -- In normal range, source scale greater than result

select toStartOfInterval(a, INTERVAL 1 MILLISECOND) from ( select toDateTime64('1980-12-12 12:12:12.123456', 6) AS a ); -- Non-constant argument

select toStartOfInterval(toDateTime64('1930-12-12 12:12:12.123', 3), INTERVAL 1 MILLISECOND); -- Below normal range, source scale matches result
select toStartOfInterval(toDateTime64('1930-12-12 12:12:12.12', 2), INTERVAL 1 MILLISECOND); -- Below normal range, source scale less than result
select toStartOfInterval(toDateTime64('1930-12-12 12:12:12.123456', 6), INTERVAL 1 MILLISECOND); -- Below normal range, source scale greater than result

select toStartOfInterval(toDateTime64('2220-12-12 12:12:12.123', 3), INTERVAL 1 MILLISECOND); -- Above normal range, source scale matches result
select toStartOfInterval(toDateTime64('2220-12-12 12:12:12.12', 2), INTERVAL 1 MILLISECOND); -- Above normal range, source scale less than result
select toStartOfInterval(toDateTime64('2220-12-12 12:12:12.123456', 6), INTERVAL 1 MILLISECOND); -- Above normal range, source scale greater than result


SELECT 'test add[...]seconds()';


SELECT '- test nanoseconds';
select addNanoseconds(toDateTime64('1980-12-12 12:12:12.123456789', 9), 1); -- In normal range, source scale matches result
select addNanoseconds(toDateTime64('1980-12-12 12:12:12.1234567', 7), 1); -- In normal range, source scale less than result

select addNanoseconds(a, 1) from ( select toDateTime64('1980-12-12 12:12:12.123456789', 9) AS a ); -- Non-constant argument

select addNanoseconds(toDateTime64('1930-12-12 12:12:12.123456789', 9), 1); -- Below normal range, source scale matches result
select addNanoseconds(toDateTime64('1930-12-12 12:12:12.1234567', 7), 1); -- Below normal range, source scale less than result

select addNanoseconds(toDateTime64('2220-12-12 12:12:12.123456789', 9), 1); -- Above normal range, source scale matches result
select addNanoseconds(toDateTime64('2220-12-12 12:12:12.1234567', 7), 1); -- Above normal range, source scale less than result


SELECT '- test microseconds';
select addMicroseconds(toDateTime64('1980-12-12 12:12:12.123456', 6), 1); -- In normal range, source scale matches result
select addMicroseconds(toDateTime64('1980-12-12 12:12:12.1234', 4), 1); -- In normal range, source scale less than result
select addMicroseconds(toDateTime64('1980-12-12 12:12:12.12345678', 8), 1); -- In normal range, source scale greater than result

select addMicroseconds(a, 1) from ( select toDateTime64('1980-12-12 12:12:12.123456', 6) AS a ); -- Non-constant argument

select addMicroseconds(toDateTime64('1930-12-12 12:12:12.123456', 6), 1); -- Below normal range, source scale matches result
select addMicroseconds(toDateTime64('1930-12-12 12:12:12.1234', 4), 1); -- Below normal range, source scale less than result
select addMicroseconds(toDateTime64('1930-12-12 12:12:12.12345678', 8), 1); -- Below normal range, source scale greater than result

select addMicroseconds(toDateTime64('2220-12-12 12:12:12.123456', 6), 1); -- Above normal range, source scale matches result
select addMicroseconds(toDateTime64('2220-12-12 12:12:12.1234', 4), 1); -- Above normal range, source scale less than result
select addMicroseconds(toDateTime64('2220-12-12 12:12:12.12345678', 8), 1); -- Above normal range, source scale greater than result


SELECT '- test milliseconds';
select addMilliseconds(toDateTime64('1980-12-12 12:12:12.123', 3), 1); -- In normal range, source scale matches result
select addMilliseconds(toDateTime64('1980-12-12 12:12:12.12', 2), 1); -- In normal range, source scale less than result
select addMilliseconds(toDateTime64('1980-12-12 12:12:12.123456', 6), 1); -- In normal range, source scale greater than result

select addMilliseconds(a, 1) from ( select toDateTime64('1980-12-12 12:12:12.123', 3) AS a ); -- Non-constant argument

select addMilliseconds(toDateTime64('1930-12-12 12:12:12.123', 3), 1); -- Below normal range, source scale matches result
select addMilliseconds(toDateTime64('1930-12-12 12:12:12.12', 2), 1); -- Below normal range, source scale less than result
select addMilliseconds(toDateTime64('1930-12-12 12:12:12.123456', 6), 1); -- Below normal range, source scale greater than result

select addMilliseconds(toDateTime64('2220-12-12 12:12:12.123', 3), 1); -- Above normal range, source scale matches result
select addMilliseconds(toDateTime64('2220-12-12 12:12:12.12', 2), 1); -- Above normal range, source scale less than result
select addMilliseconds(toDateTime64('2220-12-12 12:12:12.123456', 6), 1); -- Above normal range, source scale greater than result
