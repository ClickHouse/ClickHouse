
SET session_timezone = 'UTC';

SELECT toDateTime64('1970-01-01 00:00:01', 3)
FROM remote('127.0.0.{1,2}', system, one)
;

SELECT toDateTime64('1970-01-01 00:00:01', 3), dummy
FROM remote('127.0.0.{1,2}', system, one)
GROUP BY dummy
ORDER BY dummy
;

SELECT materialize(toDateTime64('1970-01-01 00:00:01', 3)), dummy
FROM remote('127.0.0.{1,2}', system, one)
GROUP BY dummy
ORDER BY dummy
;


SELECT toDateTime64('1970-01-01 00:00:01', 3), sum(dummy), hostname()
FROM remote('127.0.0.{1,2}', system, one)
GROUP BY hostname()
ORDER BY ALL
;
