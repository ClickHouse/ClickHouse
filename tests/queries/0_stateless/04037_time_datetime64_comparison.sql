-- Comparing Time/Time64 with DateTime/DateTime64 promotes Time to DateTime

SET session_timezone = 'UTC';
SET enable_analyzer = 1;

SELECT 'Time promoted to DateTime64 - epoch date prepended';
SELECT CAST('14:45:40', 'Time') = toDateTime64('1970-01-01 14:45:40', 9);
SELECT CAST('14:45:40', 'Time') = toDateTime64('1900-01-01 14:45:40', 9);
SELECT CAST('14:45:40', 'Time') = toDateTime64('2025-06-01 14:45:40', 9);
SELECT CAST('14:45:40', 'Time') < toDateTime64('2025-06-01 14:45:40', 9);

SELECT 'Time64 promoted to DateTime64';
SELECT CAST('14:45:40.123', 'Time64(3)') = toDateTime64('1970-01-01 14:45:40.123', 3);
SELECT CAST('14:45:40.123', 'Time64(3)') = toDateTime64('2025-06-01 14:45:40.123', 3);

SELECT 'DateTime vs Time - Time promoted to DateTime';
SELECT toDateTime('1970-01-01 10:10:10') = CAST('10:10:10', 'Time');
SELECT toDateTime('2025-01-01 10:10:10') = CAST('10:10:10', 'Time');
SELECT toDateTime('2025-01-01 10:10:10') > CAST('10:10:10', 'Time');

SELECT 'DateTime vs Time64';
SELECT toDateTime('1970-01-01 14:45:40') = CAST('14:45:40.000', 'Time64(3)');

SELECT 'Time >24h promoted to DateTime64';
SELECT CAST('25:00:00', 'Time') = toDateTime('1970-01-02 01:00:00');
SELECT CAST('48:00:00', 'Time') = toDateTime('1970-01-03 00:00:00');
SELECT CAST(CAST('25:00:00', 'Time'), 'DateTime64(3)');
SELECT CAST(CAST('999:59:59', 'Time'), 'DateTime64(0)');

SELECT 'Time vs Time64';
SELECT CAST('14:45:40', 'Time') = CAST('14:45:40.000', 'Time64(3)');

SELECT 'DateTime vs DateTime64';
SELECT toDateTime('2025-01-01 14:45:40') = toDateTime64('2025-01-01 14:45:40', 3);

SELECT 'Date vs Time - should error';
SELECT toDate('2025-01-01') = CAST('14:45:40', 'Time'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT 'Non-UTC timezone';
SET session_timezone = 'Asia/Istanbul';
-- Time '14:45:40' promotes to 1970-01-01 14:45:40 UTC, but
-- DateTime64('1970-01-01 14:45:40', 'Asia/Istanbul') is local Istanbul time, should not match
SELECT CAST('14:45:40', 'Time') = toDateTime64('1970-01-01 14:45:40', 3, 'Asia/Istanbul');
SELECT CAST('14:45:40.500', 'Time64(3)') = toDateTime64('1970-01-01 14:45:40.500', 3, 'Asia/Istanbul');
-- common type must preserve the explicit timezone from DateTime64
SELECT toTypeName(if(1, toTime64('00:00:00', 6), toDateTime64('2020-01-01 00:00:00', 3, 'Asia/Istanbul')));

SELECT CAST('14:45:40', 'Time') = toDateTime64('1970-01-01 14:45:40', 3, 'UTC');

-- DateTime64('1970-01-01 17:45:40', 'Etc/GMT-3') = 17:45:40 at UTC+3 = 14:45:40 UTC == Time '14:45:40' 
SELECT CAST('14:45:40', 'Time') = toDateTime64('1970-01-01 17:45:40', 3, 'Etc/GMT-3');
-- should not match, different time if we consider timezones
SELECT CAST('14:45:40', 'Time') = toDateTime64('1970-01-01 14:45:40', 3, 'Etc/GMT-3');

SET session_timezone = 'UTC';

SELECT 'Filtering via table';
DROP TABLE IF EXISTS test_time_cmp;
CREATE TABLE test_time_cmp (t Time) ENGINE = MergeTree ORDER BY t;
INSERT INTO test_time_cmp VALUES ('14:45:40');
SELECT * FROM test_time_cmp WHERE t = toDateTime64('1970-01-01 14:45:40', 9);
SELECT * FROM test_time_cmp WHERE t = toDateTime64('2025-06-01 14:45:40', 3);
SELECT * FROM test_time_cmp WHERE t = CAST('14:45:40', 'Time');
DROP TABLE test_time_cmp;
