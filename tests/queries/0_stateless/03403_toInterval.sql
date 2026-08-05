SELECT toInterval(5, 'nanosecond') as interval, toDateTime64('2025-01-01 00:00:00', 9) + interval AS res;
SELECT toInterval(5, 'microsecond') as interval, toDateTime64('2025-01-01 00:00:00', 9) + interval AS res;
SELECT toInterval(5, 'millisecond') as interval, toDateTime64('2025-01-01 00:00:00', 9) + interval AS res;
SELECT toInterval(5, 'second') as interval, toDateTime('2025-01-01 00:00:00') + interval AS res;
SELECT toInterval(5, 'Second') as interval, toDateTime('2025-01-01 00:00:00') + interval AS res;
SELECT toInterval(5, 'SECOND') as interval, toDateTime('2025-01-01 00:00:00') + interval AS res;
SELECT toInterval(5, 'Minute') as interval, toDateTime('2025-01-01 00:00:00') + interval AS res;
SELECT toInterval(5, 'Hour') as interval, toDateTime('2025-01-01 00:00:00') + interval AS res;
SELECT toInterval(5, 'Day') as interval, toDateTime('2025-01-01 00:00:00') + interval AS res;
SELECT toInterval(5, 'Week') as interval, toDateTime('2025-01-01 00:00:00') + interval AS res;
SELECT toInterval(5, 'Month') as interval, toDateTime('2025-01-01 00:00:00') + interval AS res;
SELECT toInterval(5, 'Quarter') as interval, toDateTime('2025-01-01 00:00:00') + interval AS res;
SELECT toInterval(5, 'Year') as interval, toDateTime('2025-01-01 00:00:00') + interval AS res;
SELECT toDateTime('2025-01-01 00:00:00') + toInterval(5, 'Year') AS res;
SELECT toDateTime('2025-01-01 00:00:00') + toInterval(number, 'second') FROM numbers(5);
SELECT toDateTime('2025-01-01 00:00:00') + toInterval(null, 'second');
SELECT toDateTime('2025-01-01 00:00:01') + toInterval(-1, 'second');
SELECT toDateTime('2025-01-01 00:00:00') + toInterval(0, 'second');
SELECT toDateTime('2025-01-01 00:00:00') + toInterval(1.5, 'second');
SELECT toDateTime('2025-01-01 00:00:00') + toInterval('5', 'second');

SELECT toInterval(); -- { serverError 42}
SELECT toInterval(''); -- { serverError 42}
SELECT toInterval('second'); -- { serverError 42 }
SELECT toInterval(5, 'second', 10); -- { serverError 42 }

SELECT toInterval(10, 5); -- { serverError 43 }

SELECT toInterval(5, ''); -- { serverError 36 }
SELECT toInterval(5, 'invalid kind'); -- { serverError 36 }
