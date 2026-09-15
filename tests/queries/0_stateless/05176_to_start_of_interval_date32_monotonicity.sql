-- Tags: no-parallel-replicas
-- https://github.com/ClickHouse/ClickHouse/issues/116945
-- The saturation added for `toStartOfInterval` covers `DateTime64` arguments only, so a `Date32`
-- argument outside the standard-precision result range is still narrowed by a plain cast and wraps.
-- The function reported itself as always monotonic anyway, and primary-key analysis mis-pruned in
-- both directions: rows silently disappeared, and `count()` overcounted through the exact-ranges
-- path.

SET session_timezone = 'UTC';

DROP TABLE IF EXISTS t_interval_date32;
CREATE TABLE t_interval_date32 (d Date32) ENGINE = MergeTree ORDER BY d
    SETTINGS index_granularity = 1, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;
INSERT INTO t_interval_date32 VALUES ('1900-01-01'),('1969-12-31'),('2000-01-01'),('2149-06-06'),('2200-01-01'),('2299-12-31');

SELECT count() FROM t_interval_date32 WHERE toStartOfInterval(d, INTERVAL 1 DAY) >= toDateTime('2050-01-01', 'UTC');
SELECT countIf(toStartOfInterval(d, INTERVAL 1 DAY) >= toDateTime('2050-01-01', 'UTC')) FROM t_interval_date32;
SELECT count() FROM t_interval_date32 WHERE toStartOfInterval(d, INTERVAL 1 YEAR) >= toDate('2100-01-01');
SELECT countIf(toStartOfInterval(d, INTERVAL 1 YEAR) >= toDate('2100-01-01')) FROM t_interval_date32;
SELECT count() FROM t_interval_date32 WHERE toStartOfInterval(d, INTERVAL 1 MONTH) >= toDate('2100-01-01');
SELECT countIf(toStartOfInterval(d, INTERVAL 1 MONTH) >= toDate('2100-01-01')) FROM t_interval_date32;
SELECT count() FROM t_interval_date32 WHERE dateTrunc('day', d) >= toDateTime('2050-01-01', 'UTC') SETTINGS function_date_trunc_return_type_behavior = 1;
SELECT countIf(dateTrunc('day', d) >= toDateTime('2050-01-01', 'UTC')) FROM t_interval_date32 SETTINGS function_date_trunc_return_type_behavior = 1;

SELECT 'the index is still used when the whole range fits the result type';
DROP TABLE IF EXISTS t_interval_in_range;
CREATE TABLE t_interval_in_range (d Date32) ENGINE = MergeTree ORDER BY d
    SETTINGS index_granularity = 1, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;
INSERT INTO t_interval_in_range SELECT toDate32('2000-01-01') + number FROM numbers(100);
SELECT count() FROM t_interval_in_range WHERE toStartOfInterval(d, INTERVAL 1 DAY) >= toDateTime('2000-03-01', 'UTC') SETTINGS force_primary_key = 1;
SELECT countIf(toStartOfInterval(d, INTERVAL 1 DAY) >= toDateTime('2000-03-01', 'UTC')) FROM t_interval_in_range;
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT d FROM t_interval_in_range WHERE toStartOfInterval(d, INTERVAL 1 DAY) >= toDateTime('2000-03-01', 'UTC')) WHERE explain LIKE '%Granules: 41/100%';

SELECT 'a DateTime64 argument saturates and stays monotonic';
DROP TABLE IF EXISTS t_interval_dt64;
CREATE TABLE t_interval_dt64 (d DateTime64(3)) ENGINE = MergeTree ORDER BY d
    SETTINGS index_granularity = 1, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;
INSERT INTO t_interval_dt64 VALUES ('1900-01-01 00:00:00.000'),('2000-01-01 00:00:00.000'),('2200-01-01 00:00:00.000');
SELECT count() FROM t_interval_dt64 WHERE toStartOfInterval(d, INTERVAL 1 YEAR) >= toDate('2100-01-01') SETTINGS force_primary_key = 1;
SELECT countIf(toStartOfInterval(d, INTERVAL 1 YEAR) >= toDate('2100-01-01')) FROM t_interval_dt64;

SELECT 'a Date result keeps the index up to its own 2149-06-06 limit, not the narrower DateTime one';
DROP TABLE IF EXISTS t_interval_date_result;
CREATE TABLE t_interval_date_result (d Date32) ENGINE = MergeTree ORDER BY d
    SETTINGS index_granularity = 1, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;
INSERT INTO t_interval_date_result SELECT toDate32('2107-01-01') + number * 366 FROM numbers(30);
SELECT count() FROM t_interval_date_result WHERE toStartOfInterval(d, INTERVAL 1 YEAR) >= toDate('2120-01-01') SETTINGS force_primary_key = 1;
SELECT countIf(toStartOfInterval(d, INTERVAL 1 YEAR) >= toDate('2120-01-01')) FROM t_interval_date_result;
SELECT count() FROM t_interval_date_result WHERE toStartOfInterval(d, INTERVAL 1 MONTH) >= toDate('2120-01-01') SETTINGS force_primary_key = 1;
SELECT countIf(toStartOfInterval(d, INTERVAL 1 MONTH) >= toDate('2120-01-01')) FROM t_interval_date_result;

SELECT 'a widened result holds the whole Date32 domain and stays monotonic';
SELECT count() FROM t_interval_date32 WHERE toStartOfInterval(d, INTERVAL 1 YEAR) >= toDate32('2100-01-01')
    SETTINGS enable_extended_results_for_datetime_functions = 1, force_primary_key = 1;
SELECT countIf(toStartOfInterval(d, INTERVAL 1 YEAR) >= toDate32('2100-01-01')) FROM t_interval_date32
    SETTINGS enable_extended_results_for_datetime_functions = 1;
SELECT count() FROM t_interval_date32 WHERE toStartOfInterval(d, INTERVAL 1 DAY) >= toDateTime64('2050-01-01', 0, 'UTC')
    SETTINGS enable_extended_results_for_datetime_functions = 1, force_primary_key = 1;
SELECT countIf(toStartOfInterval(d, INTERVAL 1 DAY) >= toDateTime64('2050-01-01', 0, 'UTC')) FROM t_interval_date32
    SETTINGS enable_extended_results_for_datetime_functions = 1;
-- The `origin` overload never reaches index analysis, so this only checks that the widened result is correct.
SELECT count() FROM t_interval_date32 WHERE toStartOfInterval(d, INTERVAL 1 WEEK, toDate32('1900-01-01')) >= toDate32('2100-01-01');
SELECT countIf(toStartOfInterval(d, INTERVAL 1 WEEK, toDate32('1900-01-01')) >= toDate32('2100-01-01')) FROM t_interval_date32;
SELECT count() FROM t_interval_date32 WHERE dateTrunc('year', d) >= toDate32('2100-01-01')
    SETTINGS function_date_trunc_return_type_behavior = 0, force_primary_key = 1;
SELECT countIf(dateTrunc('year', d) >= toDate32('2100-01-01')) FROM t_interval_date32
    SETTINGS function_date_trunc_return_type_behavior = 0;

SELECT 'a week rounding just after the epoch reaches back before it and wraps as well';
-- The week alignment of the rounding starts on 1970-01-05, so the days before it round down to
-- 1969-12-29 and the `UInt16` `Date` result wraps that to 2149-06-04. A granule that spans the wrap
-- was mapped through the rounding as if it were increasing, which prunes it away entirely.
DROP TABLE IF EXISTS t_interval_epoch;
CREATE TABLE t_interval_epoch (d Date32) ENGINE = MergeTree ORDER BY d
    SETTINGS index_granularity = 8192, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;
INSERT INTO t_interval_epoch SELECT toDate32('1970-01-01') + number FROM numbers(10);
SELECT toStartOfInterval(toDate32('1970-01-01'), INTERVAL 1 WEEK);
SELECT count() FROM t_interval_epoch WHERE toStartOfInterval(d, INTERVAL 1 WEEK) = toDate('2149-06-04');
SELECT countIf(toStartOfInterval(d, INTERVAL 1 WEEK) = toDate('2149-06-04')) FROM t_interval_epoch;
SELECT count() FROM t_interval_epoch WHERE toStartOfInterval(d, INTERVAL 1 WEEK) >= toDate('2100-01-01');
SELECT countIf(toStartOfInterval(d, INTERVAL 1 WEEK) >= toDate('2100-01-01')) FROM t_interval_epoch;
SELECT count() FROM t_interval_epoch WHERE toStartOfInterval(d, INTERVAL 1 WEEK) <= toDate('1970-01-05');
SELECT countIf(toStartOfInterval(d, INTERVAL 1 WEEK) <= toDate('1970-01-05')) FROM t_interval_epoch;

DROP TABLE t_interval_date32;
DROP TABLE t_interval_in_range;
DROP TABLE t_interval_dt64;
DROP TABLE t_interval_date_result;
DROP TABLE t_interval_epoch;
