-- Regression test: `toDate` over a `Time64` key must not be treated as monotonic.
-- Negative `Time64` values wrap to a date after 2149 while zero becomes 1970-01-01,
-- so a monotonicity claim would make key-condition pruning lose matching granules.

DROP TABLE IF EXISTS t_time64_date_mono;

CREATE TABLE t_time64_date_mono (t Time64(0)) ENGINE = MergeTree ORDER BY t
    SETTINGS index_granularity = 1, min_bytes_for_wide_part = 0;

INSERT INTO t_time64_date_mono VALUES ('-00:00:10'), ('-00:00:01'), ('00:00:00'), ('00:00:01'), ('00:00:10');

-- The two negative values wrap past 2100-01-01.
SELECT count() FROM t_time64_date_mono WHERE toDate(t) >= toDate('2100-01-01');

-- The three non-negative values stay at the epoch.
SELECT count() FROM t_time64_date_mono WHERE toDate(t) <= toDate('1970-01-01');

-- Point lookup on the non-negative side of the wrap.
SELECT count() FROM t_time64_date_mono WHERE toDate(t) = toDate('1970-01-01');

DROP TABLE t_time64_date_mono;
