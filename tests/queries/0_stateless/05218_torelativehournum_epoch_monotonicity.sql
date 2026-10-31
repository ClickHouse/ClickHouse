-- https://github.com/ClickHouse/ClickHouse/issues/119578
-- In a time zone with a whole-hour offset, `toRelativeHourNum` counts a time before the Unix epoch with a bias of a day,
-- so `1969-12-31 23:00:00` UTC is 23 and `1970-01-01 00:00:00` is 0. The function used to be reported as always
-- monotonic, so partition pruning and primary key analysis dropped rows of a range across the epoch. The values are
-- stored in partition IDs and in primary keys of existing parts, so they are kept, and only the monotonicity is fixed.

SELECT 'values, UTC';
SELECT groupArray(toRelativeHourNum(dt))
FROM (SELECT toDateTime64('1969-12-31 21:00:00', 0, 'UTC') + INTERVAL number HOUR AS dt FROM numbers(6));

SELECT 'values, Asia/Kolkata';
SELECT groupArray(toRelativeHourNum(dt))
FROM (SELECT toDateTime64('1969-12-31 21:00:00', 0, 'Asia/Kolkata') + INTERVAL number HOUR AS dt FROM numbers(6));

SELECT 'values, Date32 in America/New_York';
SELECT groupArray(toRelativeHourNum(d, 'America/New_York')) FROM (SELECT toDate32('1969-12-30') + number AS d FROM numbers(3));

DROP TABLE IF EXISTS t_relative_hour_partition;
CREATE TABLE t_relative_hour_partition (d DateTime64(0, 'UTC'))
ENGINE = MergeTree
PARTITION BY toRelativeHourNum(d)
ORDER BY d;

INSERT INTO t_relative_hour_partition VALUES
    ('1969-12-31 21:00:00'), ('1969-12-31 22:00:00'), ('1969-12-31 23:00:00'),
    ('1970-01-01 02:00:00'), ('1970-01-05 00:00:00');

SELECT 'partition IDs';
SELECT groupArray(partition) FROM (SELECT partition FROM system.parts WHERE database = currentDatabase() AND table = 't_relative_hour_partition' AND active ORDER BY partition);

SELECT 'partition pruning across the epoch';
SELECT
    (SELECT countIf(d >= toDateTime64('1969-12-31 21:00:00', 0, 'UTC') AND d <= toDateTime64('1970-01-01 02:00:00', 0, 'UTC'))
     FROM t_relative_hour_partition) AS full_scan_count,
    (SELECT count() FROM t_relative_hour_partition
     WHERE d >= toDateTime64('1969-12-31 21:00:00', 0, 'UTC') AND d <= toDateTime64('1970-01-01 02:00:00', 0, 'UTC')
     SETTINGS force_index_by_date = 1) AS indexed_count;
SELECT
    (SELECT countIf(toRelativeHourNum(d) = 23) FROM t_relative_hour_partition) AS full_scan_count,
    (SELECT count() FROM t_relative_hour_partition WHERE toRelativeHourNum(d) = 23 SETTINGS force_index_by_date = 1) AS indexed_count;

DROP TABLE t_relative_hour_partition;

DROP TABLE IF EXISTS t_relative_hour_pk;
CREATE TABLE t_relative_hour_pk (d DateTime64(0, 'UTC'))
ENGINE = MergeTree ORDER BY d
SETTINGS index_granularity = 2;

INSERT INTO t_relative_hour_pk VALUES
    ('1969-12-31 20:00:00'), ('1969-12-31 21:00:00'), ('1969-12-31 23:00:00'),
    ('1970-01-01 01:30:00'), ('1970-01-01 05:00:00'), ('1970-01-01 07:00:00');

SELECT 'primary key analysis across the epoch';
SELECT
    (SELECT countIf(toRelativeHourNum(d) = 23) FROM t_relative_hour_pk) AS full_scan_count,
    (SELECT count() FROM t_relative_hour_pk WHERE toRelativeHourNum(d) = 23 SETTINGS force_primary_key = 1) AS indexed_count;
SELECT
    (SELECT countIf(toRelativeHourNum(d) BETWEEN 1 AND 5) FROM t_relative_hour_pk) AS full_scan_count,
    (SELECT count() FROM t_relative_hour_pk WHERE toRelativeHourNum(d) BETWEEN 1 AND 5 SETTINGS force_primary_key = 1) AS indexed_count;

DROP TABLE t_relative_hour_pk;

-- A negative `DateTime64` with a fraction is rounded down to a second before the epoch, so it is on the same side.
DROP TABLE IF EXISTS t_relative_hour_pk_ms;
CREATE TABLE t_relative_hour_pk_ms (d DateTime64(3, 'UTC'))
ENGINE = MergeTree ORDER BY d
SETTINGS index_granularity = 1;

INSERT INTO t_relative_hour_pk_ms VALUES ('1969-12-31 23:59:59.000'), ('1969-12-31 23:59:59.999'), ('1970-01-01 00:00:00.000');

SELECT 'DateTime64(3) primary key analysis across the epoch';
SELECT groupArray(toRelativeHourNum(d)) FROM (SELECT d FROM t_relative_hour_pk_ms ORDER BY d);
SELECT
    (SELECT countIf(toRelativeHourNum(d) = 23) FROM t_relative_hour_pk_ms) AS full_scan_count,
    (SELECT count() FROM t_relative_hour_pk_ms WHERE toRelativeHourNum(d) = 23 SETTINGS force_primary_key = 1) AS indexed_count;

DROP TABLE t_relative_hour_pk_ms;

-- A sorting key of `toRelativeHourNum(d)`, with a condition on `d`.
DROP TABLE IF EXISTS t_relative_hour_key;
CREATE TABLE t_relative_hour_key (d DateTime64(0, 'UTC'))
ENGINE = MergeTree ORDER BY toRelativeHourNum(d)
SETTINGS index_granularity = 1;

INSERT INTO t_relative_hour_key VALUES
    ('1969-12-31 21:00:00'), ('1969-12-31 22:00:00'), ('1969-12-31 23:00:00'),
    ('1970-01-01 02:00:00'), ('1970-01-05 00:00:00');

SELECT 'sorting key analysis across the epoch';
SELECT
    (SELECT countIf(d >= toDateTime64('1969-12-31 22:00:00', 0, 'UTC') AND d <= toDateTime64('1970-01-01 02:00:00', 0, 'UTC'))
     FROM t_relative_hour_key) AS full_scan_count,
    (SELECT count() FROM t_relative_hour_key
     WHERE d >= toDateTime64('1969-12-31 22:00:00', 0, 'UTC') AND d <= toDateTime64('1970-01-01 02:00:00', 0, 'UTC')) AS indexed_count;
-- In UTC the function is not monotonic across the epoch, so a condition on `d` cannot be moved to the key.
SELECT count() FROM t_relative_hour_key
WHERE d >= toDateTime64('1969-12-31 22:00:00', 0, 'UTC') AND d <= toDateTime64('1970-01-01 02:00:00', 0, 'UTC')
SETTINGS force_primary_key = 1; -- { serverError INDEX_NOT_USED }

DROP TABLE t_relative_hour_key;

-- In a time zone with a fractional offset the same formula is used on both sides of the epoch, so the function stays
-- monotonic and the key is still used for a range across the epoch.
DROP TABLE IF EXISTS t_relative_hour_key_kolkata;
CREATE TABLE t_relative_hour_key_kolkata (d DateTime64(0, 'Asia/Kolkata'))
ENGINE = MergeTree ORDER BY toRelativeHourNum(d)
SETTINGS index_granularity = 1;

INSERT INTO t_relative_hour_key_kolkata VALUES
    ('1970-01-01 02:00:00'), ('1970-01-01 03:00:00'), ('1970-01-01 05:00:00'),
    ('1970-01-01 07:00:00'), ('1970-01-05 00:00:00');

SELECT 'sorting key analysis across the epoch, Asia/Kolkata';
SELECT groupArray(toRelativeHourNum(d)) FROM (SELECT d FROM t_relative_hour_key_kolkata ORDER BY d);
SELECT
    (SELECT countIf(d >= toDateTime64('1970-01-01 03:00:00', 0, 'Asia/Kolkata') AND d <= toDateTime64('1970-01-01 07:00:00', 0, 'Asia/Kolkata'))
     FROM t_relative_hour_key_kolkata) AS full_scan_count,
    (SELECT count() FROM t_relative_hour_key_kolkata
     WHERE d >= toDateTime64('1970-01-01 03:00:00', 0, 'Asia/Kolkata') AND d <= toDateTime64('1970-01-01 07:00:00', 0, 'Asia/Kolkata')
     SETTINGS force_primary_key = 1) AS indexed_count;

DROP TABLE t_relative_hour_key_kolkata;

-- `Date32` in a time zone west of UTC: `1969-12-31` is 10 and `1970-01-01` is 5 in `America/New_York`.
DROP TABLE IF EXISTS t_relative_hour_date32;
CREATE TABLE t_relative_hour_date32 (d Date32)
ENGINE = MergeTree ORDER BY d
SETTINGS index_granularity = 1;

INSERT INTO t_relative_hour_date32 VALUES ('1969-12-30'), ('1969-12-31'), ('1970-01-01'), ('1970-01-02');

SELECT 'Date32 primary key analysis across the epoch';
SELECT groupArray(toRelativeHourNum(d)) FROM t_relative_hour_date32 SETTINGS session_timezone = 'America/New_York';
SELECT
    (SELECT countIf(toRelativeHourNum(d) = 10) FROM t_relative_hour_date32) AS full_scan_count,
    (SELECT count() FROM t_relative_hour_date32 WHERE toRelativeHourNum(d) = 10 SETTINGS force_primary_key = 1) AS indexed_count
SETTINGS session_timezone = 'America/New_York';

DROP TABLE t_relative_hour_date32;

-- `Date` in a time zone with an offset of more than 12 hours: `1970-01-01` wraps around to a large number.
DROP TABLE IF EXISTS t_relative_hour_date;
CREATE TABLE t_relative_hour_date (d Date)
ENGINE = MergeTree ORDER BY d
SETTINGS index_granularity = 1;

INSERT INTO t_relative_hour_date VALUES ('1970-01-01'), ('1970-01-02'), ('1970-01-03');

SELECT 'Date primary key analysis after 1970-01-01';
SELECT groupArray(toRelativeHourNum(d)) FROM t_relative_hour_date SETTINGS session_timezone = 'Pacific/Tongatapu';
SELECT
    (SELECT countIf(toRelativeHourNum(d) = 11) FROM t_relative_hour_date) AS full_scan_count,
    (SELECT count() FROM t_relative_hour_date WHERE toRelativeHourNum(d) = 11 SETTINGS force_primary_key = 1) AS indexed_count
SETTINGS session_timezone = 'Pacific/Tongatapu';

DROP TABLE t_relative_hour_date;
