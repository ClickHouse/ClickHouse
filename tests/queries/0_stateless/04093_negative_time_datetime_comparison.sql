SET session_timezone = 'UTC';

SELECT CAST('-01:00:00', 'Time') < toDateTime('1970-01-01 01:00:00');
SELECT CAST('-01:00:00', 'Time') = toDateTime('1970-01-01 01:00:00');
SELECT CAST('-23:59:59', 'Time') < toDateTime('1970-01-01 00:00:00');
SELECT (CAST('01:00:00', 'Time') - INTERVAL 2 HOUR) < toDateTime('1970-01-01 01:00:00');
