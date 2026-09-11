-- Test: WITH FILL STALENESS + sorting prefix + cross-chunk boundaries
-- When ORDER BY has a non-fill prefix column, the code takes the sorting prefix path.
-- Combined with small index_granularity to force multi-chunk reads, this exercises
-- the staleness constraint preservation across chunk boundaries with sorting prefix.

SET session_timezone = 'UTC';

DROP TABLE IF EXISTS t_staleness_prefix;
CREATE TABLE t_staleness_prefix (grp String, ts DateTime, val UInt64)
ENGINE = MergeTree ORDER BY (grp, ts)
SETTINGS index_granularity = 3;

-- Insert 6 rows per group to get 2 granules per group
INSERT INTO t_staleness_prefix
SELECT 'A', toDateTime('2024-01-01 00:00:00') + number * 5, number * 5
FROM numbers(6);

INSERT INTO t_staleness_prefix
SELECT 'B', toDateTime('2024-01-01 00:00:00') + number * 5, number * 5
FROM numbers(6);

OPTIMIZE TABLE t_staleness_prefix FINAL;

SELECT 'asc with sorting prefix';
SELECT grp, ts, val, 'orig' AS orig
FROM t_staleness_prefix
ORDER BY grp, ts ASC WITH FILL STALENESS INTERVAL 2 SECOND INTERPOLATE (val)
SETTINGS max_block_size = 100000, max_threads = 1;

SELECT 'desc with sorting prefix';
SELECT grp, ts, val, 'orig' AS orig
FROM t_staleness_prefix
ORDER BY grp, ts DESC WITH FILL STALENESS INTERVAL -2 SECOND INTERPOLATE (val)
SETTINGS max_block_size = 100000, max_threads = 1;

DROP TABLE t_staleness_prefix;
