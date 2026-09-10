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

DROP TABLE t_interval_date32;
DROP TABLE t_interval_in_range;
DROP TABLE t_interval_dt64;
