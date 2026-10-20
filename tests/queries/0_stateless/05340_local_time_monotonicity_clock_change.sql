-- Random settings limits: optimize_read_in_order=(1, 1)
-- Key analysis must not treat a function of the local time as monotonic across a backward jump of the clock.

DROP TABLE IF EXISTS t_ny;
DROP TABLE IF EXISTS t_ny64;
DROP TABLE IF EXISTS t_ny_key;
DROP TABLE IF EXISTS t_utc_ny;
DROP TABLE IF EXISTS t_lord_howe;
DROP TABLE IF EXISTS t_chatham;
DROP TABLE IF EXISTS t_casey;
DROP TABLE IF EXISTS t_ny_long;
DROP TABLE IF EXISTS t_ny_sec;
DROP TABLE IF EXISTS t_utc_long;
DROP TABLE IF EXISTS t_date_long;

-- New York turns the clocks back from 02:00 EDT to 01:00 EST: 01:30, 01:40, 01:50 EDT, then 01:00, 01:10, 01:20 EST.
CREATE TABLE t_ny (dt DateTime('America/New_York')) ENGINE = MergeTree ORDER BY dt;
INSERT INTO t_ny SELECT toDateTime('2024-11-03 05:30:00', 'UTC') + number * 600 FROM numbers(6);
SELECT count() FROM t_ny WHERE toYYYYMMDDhhmmss(dt) = 20241103015000;
SELECT count() FROM t_ny WHERE toYYYYMMDDhhmmss(dt) = 20241103011000;
SELECT count() FROM t_ny WHERE toYYYYMMDDhhmmss(dt) >= 20241103014000;
SELECT count() FROM t_ny WHERE toYYYYMMDDhhmmss(dt) BETWEEN 20241103010000 AND 20241103012000;
SELECT toYYYYMMDDhhmmss(dt) FROM t_ny ORDER BY toYYYYMMDDhhmmss(dt) LIMIT 2;

CREATE TABLE t_ny64 (dt DateTime64(3, 'America/New_York')) ENGINE = MergeTree ORDER BY dt;
INSERT INTO t_ny64 SELECT toDateTime64('2024-11-03 05:30:00.250', 3, 'UTC') + toIntervalSecond(number * 600) FROM numbers(6);
SELECT count() FROM t_ny64 WHERE toYYYYMMDDhhmmss(dt) = 20241103015000;

-- The same rows in a `DateTime('UTC')` column, read in New York through the time zone argument.
CREATE TABLE t_utc_ny (dt DateTime('UTC')) ENGINE = MergeTree ORDER BY dt;
INSERT INTO t_utc_ny SELECT * FROM t_ny;
SELECT count() FROM t_utc_ny WHERE toYYYYMMDDhhmmss(dt, 'America/New_York') = 20241103015000;
SELECT toYYYYMMDDhhmmss(dt, 'America/New_York') FROM t_utc_ny ORDER BY toYYYYMMDDhhmmss(dt, 'America/New_York') LIMIT 1;

CREATE TABLE t_ny_key (dt DateTime('America/New_York')) ENGINE = MergeTree ORDER BY toYYYYMMDDhhmmss(dt);
INSERT INTO t_ny_key SELECT * FROM t_ny;
SELECT count() FROM t_ny_key WHERE dt >= toDateTime('2024-11-03 05:55:00', 'UTC');

-- Lord Howe Island turns the clocks back by 30 minutes, from 02:00 to 01:30: 01:50 and 02:10 come after it.
CREATE TABLE t_lord_howe (dt DateTime('Australia/Lord_Howe')) ENGINE = MergeTree ORDER BY dt;
INSERT INTO t_lord_howe VALUES (toDateTime('2024-04-06 15:20:00', 'UTC')), (toDateTime('2024-04-06 15:40:00', 'UTC'));
SELECT count() FROM t_lord_howe WHERE toMinute(dt) = 50;
SELECT count() FROM t_lord_howe WHERE toMinute(dt) = 10;

-- The Chatham Islands turn the clocks back from 03:45 to 02:45: 03:30, then 02:50.
CREATE TABLE t_chatham (dt DateTime('Pacific/Chatham')) ENGINE = MergeTree ORDER BY dt;
INSERT INTO t_chatham VALUES (toDateTime('2024-04-06 13:45:00', 'UTC')), (toDateTime('2024-04-06 14:05:00', 'UTC'));
SELECT count() FROM t_chatham WHERE toHour(dt) = 3;
SELECT count() FROM t_chatham WHERE toHour(dt) = 2;

-- On this day the local time of Casey runs past 24:00 before its offset change, and the hour 23 repeats.
CREATE TABLE t_casey (dt DateTime('Antarctica/Casey')) ENGINE = MergeTree ORDER BY dt;
INSERT INTO t_casey VALUES (toDateTime('2010-03-04 12:50:00', 'UTC')), (toDateTime('2010-03-04 13:10:00', 'UTC'));
SELECT toYYYYMMDDhhmmss(dt) FROM t_casey ORDER BY dt;
SELECT count() FROM t_casey WHERE toYYYYMMDDhhmmss(dt) = 20100304235000;
SELECT count() FROM t_casey WHERE toMinute(dt) = 10;

-- Ranges within one local day are still used, and any range in a zone without offset changes and for `Date`.
SET parallel_replicas_index_analysis_only_on_coordinator = 0;
CREATE TABLE t_ny_long (dt DateTime('America/New_York')) ENGINE = MergeTree ORDER BY dt SETTINGS index_granularity = 64;
INSERT INTO t_ny_long SELECT toDateTime('2024-01-01 00:00:00', 'America/New_York') + number * 60 FROM numbers(20000);
SELECT count() FROM t_ny_long WHERE toYYYYMMDDhhmmss(dt) = 20240105123400 SETTINGS max_rows_to_read = 5000, use_primary_key = 1;
SELECT count() FROM t_ny_long WHERE toHour(dt) = 12 SETTINGS max_rows_to_read = 5000, use_primary_key = 1;

CREATE TABLE t_ny_sec (dt DateTime('America/New_York')) ENGINE = MergeTree ORDER BY dt SETTINGS index_granularity = 32;
INSERT INTO t_ny_sec SELECT toDateTime('2024-01-05 06:00:00', 'America/New_York') + number * 10 FROM numbers(2160);
SELECT count() FROM t_ny_sec WHERE toMinute(dt) = 30 SETTINGS max_rows_to_read = 1000, use_primary_key = 1;

CREATE TABLE t_utc_long (dt DateTime('UTC')) ENGINE = MergeTree ORDER BY dt SETTINGS index_granularity = 64;
INSERT INTO t_utc_long SELECT toDateTime('2024-01-01 00:00:00', 'UTC') + number * 60 FROM numbers(20000);
SELECT count() FROM t_utc_long WHERE toYYYYMMDDhhmmss(dt) = 20240105123400 SETTINGS max_rows_to_read = 200, use_primary_key = 1;

CREATE TABLE t_date_long (d Date) ENGINE = MergeTree ORDER BY d SETTINGS index_granularity = 64;
INSERT INTO t_date_long SELECT toDate('2000-01-01') + number FROM numbers(20000);
SELECT count() FROM t_date_long WHERE toYYYYMMDDhhmmss(d) = 20240105000000 SETTINGS max_rows_to_read = 200, use_primary_key = 1;

DROP TABLE t_ny;
DROP TABLE t_ny64;
DROP TABLE t_ny_key;
DROP TABLE t_utc_ny;
DROP TABLE t_lord_howe;
DROP TABLE t_chatham;
DROP TABLE t_casey;
DROP TABLE t_ny_long;
DROP TABLE t_ny_sec;
DROP TABLE t_utc_long;
DROP TABLE t_date_long;
