-- Random settings limits: merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability=(0, 0)
-- `timezoneOffset` and `toTimeWithFixedDate` over a key range that spans a UTC offset change must not be treated as monotonic.
-- Each line with two counts prints the count with the key condition and the count computed without it; both must match.

DROP TABLE IF EXISTS t_daily;
DROP TABLE IF EXISTS t_daily64;
DROP TABLE IF EXISTS t_hourly;
DROP TABLE IF EXISTS t_fall;
DROP TABLE IF EXISTS t_fall_min;
DROP TABLE IF EXISTS t_out_of_lut;
DROP TABLE IF EXISTS t_fall64;
DROP TABLE IF EXISTS t_one_day;

-- One row per day at local midnight: the offset is -05:00 at both ends of the part and -04:00 in between.
CREATE TABLE t_daily (dt DateTime('America/New_York')) ENGINE = MergeTree ORDER BY dt SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';
INSERT INTO t_daily SELECT toDateTime(toDate('2024-01-01') + number, 'America/New_York') FROM numbers(366);
SELECT 'daily = -18000', (SELECT count() FROM t_daily WHERE timezoneOffset(dt) = -18000), (SELECT countIf(timezoneOffset(dt) = -18000) FROM t_daily);
SELECT 'daily = -14400', (SELECT count() FROM t_daily WHERE timezoneOffset(dt) = -14400), (SELECT countIf(timezoneOffset(dt) = -14400) FROM t_daily);
SELECT 'daily > -18000', (SELECT count() FROM t_daily WHERE timezoneOffset(dt) > -18000), (SELECT countIf(timezoneOffset(dt) > -18000) FROM t_daily);

CREATE TABLE t_daily64 (dt DateTime64(3, 'America/New_York')) ENGINE = MergeTree ORDER BY dt SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';
INSERT INTO t_daily64 SELECT toDateTime64(toDate('2024-01-01') + number, 3, 'America/New_York') FROM numbers(366);
SELECT 'daily64 = -18000', (SELECT count() FROM t_daily64 WHERE timezoneOffset(dt) = -18000), (SELECT countIf(timezoneOffset(dt) = -18000) FROM t_daily64);
SELECT 'daily64 = -14400', (SELECT count() FROM t_daily64 WHERE timezoneOffset(dt) = -14400), (SELECT countIf(timezoneOffset(dt) = -14400) FROM t_daily64);

-- One row per hour, both ends at 12:00 -04:00.
CREATE TABLE t_hourly (dt DateTime('America/New_York')) ENGINE = MergeTree ORDER BY dt SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';
INSERT INTO t_hourly SELECT toDateTime('2024-07-01 12:00:00', 'America/New_York') + INTERVAL number HOUR FROM numbers(8761);
SELECT 'hourly = -18000', (SELECT count() FROM t_hourly WHERE timezoneOffset(dt) = -18000), (SELECT countIf(timezoneOffset(dt) = -18000) FROM t_hourly);

-- 01:50 -04:00 and 01:10 -05:00, inside the hour that is repeated on 2024-11-03.
CREATE TABLE t_fall (dt DateTime('America/New_York')) ENGINE = MergeTree ORDER BY dt SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';
INSERT INTO t_fall VALUES (toDateTime('2024-11-03 05:50:00', 'UTC')), (toDateTime('2024-11-03 06:10:00', 'UTC'));
SELECT 'fall = 01:50', (SELECT count() FROM t_fall WHERE toTimeWithFixedDate(dt) = toDateTime('1970-01-02 01:50:00', 'America/New_York')), (SELECT countIf(toTimeWithFixedDate(dt) = toDateTime('1970-01-02 01:50:00', 'America/New_York')) FROM t_fall);

-- One row per minute across the repeated hour, in small granules.
CREATE TABLE t_fall_min (dt DateTime('America/New_York')) ENGINE = MergeTree ORDER BY dt SETTINGS index_granularity = 7, index_granularity_bytes = '10Mi';
INSERT INTO t_fall_min SELECT toDateTime('2024-11-03 04:00:00', 'UTC') + number * 60 FROM numbers(300);
SELECT 'fall_min 01:00 - 01:29', (SELECT count() FROM t_fall_min WHERE toTimeWithFixedDate(dt) BETWEEN toDateTime('1970-01-02 01:00:00', 'America/New_York') AND toDateTime('1970-01-02 01:29:00', 'America/New_York')), (SELECT countIf(toTimeWithFixedDate(dt) BETWEEN toDateTime('1970-01-02 01:00:00', 'America/New_York') AND toDateTime('1970-01-02 01:29:00', 'America/New_York')) FROM t_fall_min);
SELECT 'fall_min = 01:55', (SELECT count() FROM t_fall_min WHERE toTimeWithFixedDate(dt) = toDateTime('1970-01-02 01:55:00', 'America/New_York')), (SELECT countIf(toTimeWithFixedDate(dt) = toDateTime('1970-01-02 01:55:00', 'America/New_York')) FROM t_fall_min);
SELECT 'fall_min = -14400', (SELECT count() FROM t_fall_min WHERE timezoneOffset(dt) = -14400), (SELECT countIf(timezoneOffset(dt) = -14400) FROM t_fall_min);

-- One row per hour from 9999-12-31 23:00:00 UTC, beyond the range of the lookup table.
CREATE TABLE t_out_of_lut (dt DateTime64(0, 'UTC')) ENGINE = MergeTree ORDER BY dt SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';
INSERT INTO t_out_of_lut SELECT fromUnixTimestamp64Second(toInt64(253402297200 + number * 3600), 'UTC') FROM numbers(72);
SELECT 'out_of_lut = 05:00', (SELECT count() FROM t_out_of_lut WHERE toTimeWithFixedDate(dt) = toDateTime('1970-01-02 05:00:00', 'UTC')), (SELECT countIf(toTimeWithFixedDate(dt) = toDateTime('1970-01-02 05:00:00', 'UTC')) FROM t_out_of_lut);

-- Two rows 0.5 s apart just before the fall-back of 1969-10-26; before the epoch, `toTimeWithFixedDate` truncates the second one to 01:00:00 -05:00.
CREATE TABLE t_fall64 (dt DateTime64(3, 'America/New_York')) ENGINE = MergeTree ORDER BY dt SETTINGS index_granularity = 1, index_granularity_bytes = '10Mi';
INSERT INTO t_fall64 VALUES (toDateTime64('1969-10-26 05:59:59.000', 3, 'UTC')), (toDateTime64('1969-10-26 05:59:59.500', 3, 'UTC'));
SELECT 'fall64 = 01:59:59', (SELECT count() FROM t_fall64 WHERE toTimeWithFixedDate(dt) = toDateTime('1970-01-02 01:59:59', 'America/New_York')), (SELECT countIf(toTimeWithFixedDate(dt) = toDateTime('1970-01-02 01:59:59', 'America/New_York')) FROM t_fall64);

-- One row per hour of the local day 2024-11-03, one granule per row: ranges inside the day that keep one UTC offset are still pruned.
CREATE TABLE t_one_day (dt DateTime('America/New_York')) ENGINE = MergeTree ORDER BY dt SETTINGS index_granularity = 1, index_granularity_bytes = '10Mi';
INSERT INTO t_one_day SELECT toDateTime('2024-11-03 04:00:00', 'UTC') + INTERVAL number HOUR FROM numbers(25);
SELECT 'one_day = -14400', count() FROM t_one_day WHERE timezoneOffset(dt) = -14400 SETTINGS max_rows_to_read = 2, parallel_replicas_index_analysis_only_on_coordinator = 0;
SELECT 'one_day = 03:00', count() FROM t_one_day WHERE toTimeWithFixedDate(dt) = toDateTime('1970-01-02 03:00:00', 'America/New_York') SETTINGS max_rows_to_read = 3, parallel_replicas_index_analysis_only_on_coordinator = 0;

DROP TABLE t_daily;
DROP TABLE t_daily64;
DROP TABLE t_hourly;
DROP TABLE t_fall;
DROP TABLE t_fall_min;
DROP TABLE t_out_of_lut;
DROP TABLE t_fall64;
DROP TABLE t_one_day;
