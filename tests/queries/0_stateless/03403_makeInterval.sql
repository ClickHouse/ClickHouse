SELECT makeInterval('nanosecond', 5) as interval, toDateTime64('2025-01-01 00:00:00', 9) + interval AS res;
SELECT makeInterval('microsecond', 5) as interval, toDateTime64('2025-01-01 00:00:00', 9) + interval AS res;
SELECT makeInterval('millisecond', 5) as interval, toDateTime64('2025-01-01 00:00:00', 9) + interval AS res;
SELECT makeInterval('second', 5) as interval, toDateTime('2025-01-01 00:00:00') + interval AS res;
SELECT makeInterval('Second', 5) as interval, toDateTime('2025-01-01 00:00:00') + interval AS res;
SELECT makeInterval('SECOND', 5) as interval, toDateTime('2025-01-01 00:00:00') + interval AS res;
SELECT makeInterval('Minute', 5) as interval, toDateTime('2025-01-01 00:00:00') + interval AS res;
SELECT makeInterval('Hour', 5) as interval, toDateTime('2025-01-01 00:00:00') + interval AS res;
SELECT makeInterval('Day', 5) as interval, toDateTime('2025-01-01 00:00:00') + interval AS res;
SELECT makeInterval('Week', 5) as interval, toDateTime('2025-01-01 00:00:00') + interval AS res;
SELECT makeInterval('Month', 5) as interval, toDateTime('2025-01-01 00:00:00') + interval AS res;
SELECT makeInterval('Quarter', 5) as interval, toDateTime('2025-01-01 00:00:00') + interval AS res;
SELECT makeInterval('Year', 5) as interval, toDateTime('2025-01-01 00:00:00') + interval AS res;
SELECT toDateTime('2025-01-01 00:00:00') + makeInterval('Year', 5) AS res;
SELECT toDateTime('2025-01-01 00:00:00') + makeInterval('second', number) FROM numbers(5);
SELECT toDateTime('2025-01-01 00:00:00') + makeInterval('second', null);
SELECT toDateTime('2025-01-01 00:00:01') + makeInterval('second', -1);
SELECT toDateTime('2025-01-01 00:00:00') + makeInterval('second', 0);

SELECT makeInterval(); -- { serverError 42}
SELECT makeInterval(''); -- { serverError 42}
SELECT makeInterval('second'); -- { serverError 42 }
SELECT makeInterval('second', 5, 10); -- { serverError 42 }

SELECT makeInterval('second', '5'); -- { serverError 43 }
SELECT makeInterval(10, 5); -- { serverError 43 }
SELECT makeInterval('second', 1.5); -- { serverError 43 }

SELECT makeInterval('', 5); -- { serverError 36 }
SELECT makeInterval('invalid kind', 5); -- { serverError 36 }
