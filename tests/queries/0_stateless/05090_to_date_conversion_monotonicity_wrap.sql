-- Tags: no-parallel-replicas
-- https://github.com/ClickHouse/ClickHouse/issues/116946
-- At the default `date_time_overflow_behavior = 'ignore'`, `toDate` and `toDateTime` narrow a
-- `DateTime64` or `Date32` argument with a plain cast, so a value the result type cannot hold wraps.
-- A wrapping conversion is not monotonic, but both reported themselves as always monotonic, and
-- primary-key analysis mis-pruned in both directions: rows silently disappeared, and `count()`
-- overcounted through the exact-ranges path.

SET session_timezone = 'UTC';

DROP TABLE IF EXISTS t_wrap_dt64;
CREATE TABLE t_wrap_dt64 (d DateTime64(3)) ENGINE = MergeTree ORDER BY d
    SETTINGS index_granularity = 1, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;
INSERT INTO t_wrap_dt64 VALUES ('1900-01-01 00:00:00.000'),('1969-12-31 23:59:59.000'),('2000-01-01 00:00:00.000'),('2200-01-01 00:00:00.000'),('2262-04-11 00:00:00.000');

SELECT count() FROM t_wrap_dt64 WHERE toDate(d) >= toDate('2100-01-01');
SELECT countIf(toDate(d) >= toDate('2100-01-01')) FROM t_wrap_dt64;
SELECT count() FROM t_wrap_dt64 WHERE toDateTime(d) >= toDateTime('2050-01-01', 'UTC');
SELECT countIf(toDateTime(d) >= toDateTime('2050-01-01', 'UTC')) FROM t_wrap_dt64;
SELECT count() FROM t_wrap_dt64 WHERE toDate(d) < toDate('2000-01-01');
SELECT countIf(toDate(d) < toDate('2000-01-01')) FROM t_wrap_dt64;

DROP TABLE IF EXISTS t_wrap_date32;
CREATE TABLE t_wrap_date32 (d Date32) ENGINE = MergeTree ORDER BY d
    SETTINGS index_granularity = 1, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;
INSERT INTO t_wrap_date32 VALUES ('1900-01-01'),('1969-12-31'),('2000-01-01'),('2149-06-06'),('2200-01-01'),('2299-12-31');

SELECT count() FROM t_wrap_date32 WHERE toDate(d) >= toDate('2100-01-01');
SELECT countIf(toDate(d) >= toDate('2100-01-01')) FROM t_wrap_date32;
SELECT count() FROM t_wrap_date32 WHERE toDateTime(d) >= toDateTime('2050-01-01', 'UTC');
SELECT countIf(toDateTime(d) >= toDateTime('2050-01-01', 'UTC')) FROM t_wrap_date32;

SELECT 'the index is still used when the whole range converts without wrapping';
DROP TABLE IF EXISTS t_in_range;
CREATE TABLE t_in_range (d DateTime64(3)) ENGINE = MergeTree ORDER BY d
    SETTINGS index_granularity = 1, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;
INSERT INTO t_in_range SELECT toDateTime64('2000-01-01 00:00:00.000', 3) + INTERVAL number DAY FROM numbers(100);
SELECT count() FROM t_in_range WHERE toDate(d) >= toDate('2000-03-01') SETTINGS force_primary_key = 1;
SELECT countIf(toDate(d) >= toDate('2000-03-01')) FROM t_in_range;
SELECT count() FROM t_in_range WHERE toDateTime(d) >= toDateTime('2000-03-01', 'UTC') SETTINGS force_primary_key = 1;
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT d FROM t_in_range WHERE toDate(d) >= toDate('2000-03-01')) WHERE explain LIKE '%Granules: 41/100%';

DROP TABLE IF EXISTS t_in_range_date32;
CREATE TABLE t_in_range_date32 (d Date32) ENGINE = MergeTree ORDER BY d
    SETTINGS index_granularity = 1, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;
INSERT INTO t_in_range_date32 SELECT toDate32('2000-01-01') + number FROM numbers(100);
SELECT count() FROM t_in_range_date32 WHERE toDate(d) >= toDate('2000-03-01') SETTINGS force_primary_key = 1;
SELECT countIf(toDate(d) >= toDate('2000-03-01')) FROM t_in_range_date32;

SELECT 'the instant that wraps depends on the time zone';
-- `toDateTime` takes the instant of local midnight: day 49710 (`2106-02-07`) still fits `DateTime` in
-- `UTC`, but in `America/Hermosillo` (UTC-7) its midnight is 7 hours later and wraps to 1970.
SET session_timezone = 'America/Hermosillo';
DROP TABLE IF EXISTS t_wrap_tz_date32;
CREATE TABLE t_wrap_tz_date32 (d Date32) ENGINE = MergeTree ORDER BY d
    SETTINGS index_granularity = 1, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;
INSERT INTO t_wrap_tz_date32 VALUES ('2000-01-01'),('2106-02-05'),('2106-02-06'),('2106-02-07'),('2106-02-08');
SELECT count() FROM t_wrap_tz_date32 WHERE toDateTime(d) >= toDateTime('2050-01-01');
SELECT countIf(toDateTime(d) >= toDateTime('2050-01-01')) FROM t_wrap_tz_date32;
SELECT count() FROM t_wrap_tz_date32 WHERE toDateTime(d) < toDateTime('2050-01-01');
SELECT countIf(toDateTime(d) < toDateTime('2050-01-01')) FROM t_wrap_tz_date32;
DROP TABLE t_wrap_tz_date32;

-- `toDate` takes the local calendar day: an instant late on `2149-06-06` UTC is already `2149-06-07`
-- in `Pacific/Kiritimati` (UTC+14), which does not fit `Date` and wraps to 1970.
SET session_timezone = 'Pacific/Kiritimati';
DROP TABLE IF EXISTS t_wrap_tz_dt64;
CREATE TABLE t_wrap_tz_dt64 (d DateTime64(3)) ENGINE = MergeTree ORDER BY d
    SETTINGS index_granularity = 1, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;
INSERT INTO t_wrap_tz_dt64 SELECT toDateTime64(arrayJoin(['2000-01-01 00:00:00', '2149-06-05 00:00:00', '2149-06-06 00:00:00', '2149-06-06 12:00:00', '2149-06-07 12:00:00']), 3, 'UTC');
SELECT count() FROM t_wrap_tz_dt64 WHERE toDate(d) >= toDate('2100-01-01');
SELECT countIf(toDate(d) >= toDate('2100-01-01')) FROM t_wrap_tz_dt64;
SELECT count() FROM t_wrap_tz_dt64 WHERE toDate(d) < toDate('2100-01-01');
SELECT countIf(toDate(d) < toDate('2100-01-01')) FROM t_wrap_tz_dt64;
DROP TABLE t_wrap_tz_dt64;
SET session_timezone = 'UTC';

SELECT 'a Date argument wraps too: Date reaches 2149-06-06, DateTime ends in 2106-02-07';
DROP TABLE IF EXISTS t_wrap_date;
CREATE TABLE t_wrap_date (d Date) ENGINE = MergeTree ORDER BY d
    SETTINGS index_granularity = 1, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;
INSERT INTO t_wrap_date VALUES ('2000-01-01'),('2106-02-07'),('2106-02-08'),('2149-06-06');
SELECT count() FROM t_wrap_date WHERE toDateTime(d) >= toDateTime('2050-01-01');
SELECT countIf(toDateTime(d) >= toDateTime('2050-01-01')) FROM t_wrap_date;
SELECT count() FROM t_wrap_date WHERE toDateTime(d) < toDateTime('2050-01-01');
SELECT countIf(toDateTime(d) < toDateTime('2050-01-01')) FROM t_wrap_date;
DROP TABLE t_wrap_date;

SELECT 'the wider results are unaffected';
SELECT count() FROM t_wrap_dt64 WHERE toDate32(d) >= toDate32('2100-01-01') SETTINGS force_primary_key = 1;
SELECT countIf(toDate32(d) >= toDate32('2100-01-01')) FROM t_wrap_dt64;
SELECT count() FROM t_wrap_date32 WHERE toDateTime64(d, 3) >= toDateTime64('2100-01-01 00:00:00', 3) SETTINGS force_primary_key = 1;
SELECT countIf(toDateTime64(d, 3) >= toDateTime64('2100-01-01 00:00:00', 3)) FROM t_wrap_date32;

DROP TABLE t_wrap_dt64;
DROP TABLE t_wrap_date32;
DROP TABLE t_in_range;
DROP TABLE t_in_range_date32;
