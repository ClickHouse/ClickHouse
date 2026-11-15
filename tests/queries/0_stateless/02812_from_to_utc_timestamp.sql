DROP TABLE IF EXISTS test_tbl;
CREATE TABLE test_tbl (x UInt32, y DateTime, z DateTime64) engine=MergeTree ORDER BY x;
INSERT INTO test_tbl values(1, '2023-03-16', '2023-03-16 11:22:33');
INSERT INTO test_tbl values(2, '2023-03-16 11:22:33', '2023-03-16');
INSERT INTO test_tbl values(3, '2023-03-16 11:22:33', '2023-03-16 11:22:33.123456');
SELECT x, to_utc_timestamp(toDateTime('2023-03-16 11:22:33'), 'Etc/GMT+1'), from_utc_timestamp(toDateTime64('2023-03-16 11:22:33', 3), 'Etc/GMT+1'), to_utc_timestamp(y, 'Asia/Shanghai'), from_utc_timestamp(z, 'Asia/Shanghai') from test_tbl order by x;
-- timestamp convert between DST timezone and UTC
SELECT to_utc_timestamp(toDateTime('2024-02-24 11:22:33'), 'Europe/Madrid'), from_utc_timestamp(toDateTime('2024-02-24 11:22:33'), 'Europe/Madrid') SETTINGS session_timezone='Europe/Moscow';
SELECT to_utc_timestamp(toDateTime('2024-10-24 11:22:33'), 'Europe/Madrid'), from_utc_timestamp(toDateTime('2024-10-24 11:22:33'), 'Europe/Madrid') SETTINGS session_timezone='Europe/Moscow';
SELECT to_utc_timestamp(toDateTime('2024-10-24 11:22:33'), 'EST'), from_utc_timestamp(toDateTime('2024-10-24 11:22:33'), 'EST') SETTINGS session_timezone='Europe/Moscow';

SELECT 'leap year:', to_utc_timestamp(toDateTime('2024-02-29 11:22:33'), 'EST'), from_utc_timestamp(toDateTime('2024-02-29 11:22:33'), 'EST') SETTINGS session_timezone='Europe/Moscow';
SELECT 'non-leap year:', to_utc_timestamp(toDateTime('2023-02-29 11:22:33'), 'EST'), from_utc_timestamp(toDateTime('2023-02-29 11:22:33'), 'EST') SETTINGS session_timezone='Europe/Moscow';
SELECT 'leap year:', to_utc_timestamp(toDateTime('2024-02-28 23:22:33'), 'EST'), from_utc_timestamp(toDateTime('2024-03-01 00:22:33'), 'EST') SETTINGS session_timezone='Europe/Moscow';
SELECT 'non-leap year:', to_utc_timestamp(toDateTime('2023-02-28 23:22:33'), 'EST'), from_utc_timestamp(toDateTime('2023-03-01 00:22:33'), 'EST') SETTINGS session_timezone='Europe/Moscow';
SELECT 'timezone with half-hour offset:', to_utc_timestamp(toDateTime('2024-02-29 11:22:33'), 'Australia/Adelaide'), from_utc_timestamp(toDateTime('2024-02-29 11:22:33'), 'Australia/Adelaide') SETTINGS session_timezone='Europe/Moscow';
SELECT 'jump over a year:', to_utc_timestamp(toDateTime('2023-12-31 23:01:01'), 'EST'), from_utc_timestamp(toDateTime('2024-01-01 01:01:01'), 'EST') SETTINGS session_timezone='Europe/Moscow';

-- Test cases for dates before Unix epoch (1970-01-01)
SELECT 'before epoch 1:', to_utc_timestamp(toDateTime('1969-12-31 23:59:59'), 'UTC'), from_utc_timestamp(toDateTime('1969-12-31 23:59:59'), 'UTC') SETTINGS session_timezone='UTC';
SELECT 'before epoch 2:', to_utc_timestamp(toDateTime('1969-01-01 00:00:00'), 'UTC'), from_utc_timestamp(toDateTime('1969-01-01 00:00:00'), 'UTC') SETTINGS session_timezone='UTC';
SELECT 'before epoch 3:', to_utc_timestamp(toDateTime('1900-01-01 00:00:00'), 'UTC'), from_utc_timestamp(toDateTime('1900-01-01 00:00:00'), 'UTC') SETTINGS session_timezone='UTC';

-- Test cases for dates after maximum date (2106-02-07 06:28:15)
SELECT 'after max 1:', to_utc_timestamp(toDateTime('2106-02-07 06:28:16'), 'UTC'), from_utc_timestamp(toDateTime('2106-02-07 06:28:16'), 'UTC') SETTINGS session_timezone='UTC';
SELECT 'after max 2:', to_utc_timestamp(toDateTime('2106-02-08 00:00:00'), 'UTC'), from_utc_timestamp(toDateTime('2106-02-08 00:00:00'), 'UTC') SETTINGS session_timezone='UTC';
SELECT 'after max 3:', to_utc_timestamp(toDateTime('2107-01-01 00:00:00'), 'UTC'), from_utc_timestamp(toDateTime('2107-01-01 00:00:00'), 'UTC') SETTINGS session_timezone='UTC';

-- Test cases for dates before epoch with different timezones
SELECT 'before epoch with timezone 1:', to_utc_timestamp(toDateTime('1969-12-31 23:59:59'), 'America/New_York'), from_utc_timestamp(toDateTime('1969-12-31 23:59:59'), 'America/New_York') SETTINGS session_timezone='UTC';
SELECT 'before epoch with timezone 2:', to_utc_timestamp(toDateTime('1969-12-31 23:59:59'), 'Asia/Tokyo'), from_utc_timestamp(toDateTime('1969-12-31 23:59:59'), 'Asia/Tokyo') SETTINGS session_timezone='UTC';

-- Test cases for dates after max with different timezones
SELECT 'after max with timezone 1:', to_utc_timestamp(toDateTime('2106-02-07 06:28:16'), 'America/New_York'), from_utc_timestamp(toDateTime('2106-02-07 06:28:16'), 'America/New_York') SETTINGS session_timezone='UTC';
SELECT 'after max with timezone 2:', to_utc_timestamp(toDateTime('2106-02-07 06:28:16'), 'Asia/Tokyo'), from_utc_timestamp(toDateTime('2106-02-07 06:28:16'), 'Asia/Tokyo') SETTINGS session_timezone='UTC';

DROP TABLE test_tbl;
