-- Regression test: `toDateTime` over a `Time64` (or `Time`) key column must not be treated
-- as monotonic. `Time64` values can be negative, and with the default
-- `date_time_overflow_behavior = 'ignore'` the `Time64 -> DateTime` conversion wraps negative
-- values to the end of the `DateTime` range (e.g. `-00:00:01` becomes `2106-02-07 06:28:15`),
-- so the function drops at zero. If `ToDateTimeMonotonicity` claimed monotonicity, the key
-- condition would prune granules incorrectly and queries would silently return wrong results.

DROP TABLE IF EXISTS t_time64_dt_mono;

CREATE TABLE t_time64_dt_mono (t Time64(0)) ENGINE = MergeTree ORDER BY t
    SETTINGS index_granularity = 1, min_bytes_for_wide_part = 0;

INSERT INTO t_time64_dt_mono VALUES ('-00:00:10'), ('-00:00:01'), ('00:00:00'), ('00:00:01'), ('00:00:10');

-- The two negative values wrap past 2106-01-01.
SELECT count() FROM t_time64_dt_mono WHERE toDateTime(t) >= toDateTime('2106-01-01 00:00:00', 'UTC');

-- The three non-negative values stay at the epoch.
SELECT count() FROM t_time64_dt_mono WHERE toDateTime(t) <= toDateTime('1970-01-01 00:00:10', 'UTC');

-- Point lookups on both sides of the wrap.
SELECT count() FROM t_time64_dt_mono WHERE toDateTime(t) = toDateTime('1970-01-01 00:00:01', 'UTC');
SELECT count() FROM t_time64_dt_mono WHERE toDateTime(t) IN (toDateTime('1970-01-01 00:00:01', 'UTC'), toDateTime('2106-02-07 06:28:15', 'UTC'));

DROP TABLE t_time64_dt_mono;
