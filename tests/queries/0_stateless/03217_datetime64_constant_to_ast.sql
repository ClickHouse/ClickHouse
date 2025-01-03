
SET session_timezone = 'UTC';

SELECT toDateTime64('1970-01-01 00:00:01', 3)
FROM remote('127.0.0.{1,2}', system, one)
;
