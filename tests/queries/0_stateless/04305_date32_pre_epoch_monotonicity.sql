-- Regression test for https://github.com/ClickHouse/ClickHouse/pull/105244
--
-- toMonday/toStartOfMonth/toStartOfYear clamp pre-epoch rounding results to 1970-01-01 for the
-- Date result type. They are also used as the FactorTransform for toDayOfWeek/toDayOfMonth/toMonth,
-- where the clamp must NOT be applied: it would collapse every pre-epoch week/month/year to factor
-- 0, making getMonotonicityForRange report a wrapping function as monotonic and prune granules that
-- actually contain matching rows. Each query below would return 0 (instead of 1) if a granule whose
-- boundary keys share the same period were wrongly treated as monotonic. The countIf line is the
-- ground truth (full scan, no key pruning) and must match.

SET session_timezone = 'UTC';

-- toDayOfWeek on a Date32 key (FactorTransform = toMonday).
-- Granule 0 spans 1900-01-02 (Tue) .. 1900-01-10 (Wed); 1900-01-05 (Fri) is inside it.
DROP TABLE IF EXISTS t_dow32;
CREATE TABLE t_dow32 (d Date32) ENGINE = MergeTree ORDER BY d
    SETTINGS index_granularity = 8, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;
INSERT INTO t_dow32 SELECT toDate32('1900-01-02') + number FROM numbers(9);
SELECT count() FROM t_dow32 WHERE toDayOfWeek(d) = 5;
SELECT countIf(toDayOfWeek(d) = 5) FROM t_dow32;

-- toDayOfWeek on a DateTime64 key (exercises the TransformDateTime64 factor path).
DROP TABLE IF EXISTS t_dow64;
CREATE TABLE t_dow64 (d DateTime64(0, 'UTC')) ENGINE = MergeTree ORDER BY d
    SETTINGS index_granularity = 8, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;
INSERT INTO t_dow64 SELECT toDateTime64('1900-01-02 00:00:00', 0, 'UTC') + INTERVAL number DAY FROM numbers(9);
SELECT count() FROM t_dow64 WHERE toDayOfWeek(d) = 5;
SELECT countIf(toDayOfWeek(d) = 5) FROM t_dow64;

-- toMonth on a Date32 key (FactorTransform = toStartOfYear).
-- Granule 0 spans 1900-03-15 .. 1905-03-20 (both March); 1900-07-20 is inside it.
DROP TABLE IF EXISTS t_month32;
CREATE TABLE t_month32 (d Date32) ENGINE = MergeTree ORDER BY d
    SETTINGS index_granularity = 8, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;
INSERT INTO t_month32 VALUES ('1900-03-15'),('1900-07-20'),('1901-01-10'),('1901-05-05'),('1902-09-09'),('1903-02-02'),('1903-11-11'),('1904-06-06'),('1905-03-20');
SELECT count() FROM t_month32 WHERE toMonth(d) = 7;
SELECT countIf(toMonth(d) = 7) FROM t_month32;

-- toDayOfMonth on a Date32 key (FactorTransform = toStartOfMonth).
-- Granule 0 spans 1900-01-15 .. 1900-08-15 (both day 15); 1900-01-20 is inside it.
DROP TABLE IF EXISTS t_dom32;
CREATE TABLE t_dom32 (d Date32) ENGINE = MergeTree ORDER BY d
    SETTINGS index_granularity = 8, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;
INSERT INTO t_dom32 VALUES ('1900-01-15'),('1900-01-20'),('1900-02-15'),('1900-03-15'),('1900-04-15'),('1900-05-15'),('1900-06-15'),('1900-07-15'),('1900-08-15');
SELECT count() FROM t_dom32 WHERE toDayOfMonth(d) = 20;
SELECT countIf(toDayOfMonth(d) = 20) FROM t_dom32;

DROP TABLE t_dow32;
DROP TABLE t_dow64;
DROP TABLE t_month32;
DROP TABLE t_dom32;
