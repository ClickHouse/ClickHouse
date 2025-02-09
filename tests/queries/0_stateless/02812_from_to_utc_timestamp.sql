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

DROP TABLE test_tbl;
