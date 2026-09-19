-- https://github.com/ClickHouse/ClickHouse/issues/119578
-- toRelativeHourNum used a formula for t >= 0 (whole-hour-offset time zones) that was
-- inconsistent with the formula used for t < 0, producing a discontinuity (non-monotonicity)
-- right at the Unix epoch: e.g. for DateTime64(0, 'UTC'), 1969-12-31 23:30:00 mapped to 23 while
-- 1970-01-01 00:00:00 mapped to 0. Because the function is (incorrectly) declared always
-- monotonic, partition pruning and primary key range analysis silently dropped rows whose
-- range straddled the epoch.

SELECT 'direct monotonicity sweep, UTC (whole-hour offset, fast path): must never decrease';
-- consecutive whole hours from 21:00 the day before the epoch to 02:00 the day after, straddling t = 0
SELECT groupArray(toRelativeHourNum(dt)) AS arr, arr = arraySort(arr) AS never_decreases
FROM (
    SELECT toDateTime64('1969-12-31 21:00:00', 0, 'UTC') + INTERVAL number HOUR AS dt
    FROM numbers(6)
);

SELECT 'direct monotonicity sweep, Asia/Kolkata (+05:30, sub-hour offset, calendar path): must never decrease';
SELECT groupArray(toRelativeHourNum(dt)) AS arr, arr = arraySort(arr) AS never_decreases
FROM (
    SELECT toDateTime64('1969-12-31 21:00:00', 0, 'Asia/Kolkata') + INTERVAL number HOUR AS dt
    FROM numbers(6)
);

DROP TABLE IF EXISTS t_relative_hour_partition;
CREATE TABLE t_relative_hour_partition (d DateTime64(0, 'UTC'))
ENGINE = MergeTree
PARTITION BY toRelativeHourNum(d)
ORDER BY d;

INSERT INTO t_relative_hour_partition VALUES
    ('1969-12-31 21:00:00'), ('1969-12-31 22:00:00'), ('1969-12-31 23:00:00'),
    ('1970-01-01 02:00:00'), ('1970-01-05 00:00:00');

SELECT 'partition pruning across the epoch: indexed count must match full-scan count';
SELECT
    (SELECT countIf(d >= toDateTime64('1969-12-31 21:00:00', 0, 'UTC') AND d <= toDateTime64('1970-01-01 02:00:00', 0, 'UTC'))
     FROM t_relative_hour_partition SETTINGS force_primary_key = 0, force_index_by_date = 0) AS full_scan_count,
    (SELECT count() FROM t_relative_hour_partition
     WHERE d >= toDateTime64('1969-12-31 21:00:00', 0, 'UTC') AND d <= toDateTime64('1970-01-01 02:00:00', 0, 'UTC')
     SETTINGS force_index_by_date = 1) AS indexed_count,
    full_scan_count = indexed_count AS matches;

DROP TABLE t_relative_hour_partition;

DROP TABLE IF EXISTS t_relative_hour_pk;
CREATE TABLE t_relative_hour_pk (d DateTime64(0, 'UTC'))
ENGINE = MergeTree ORDER BY d
SETTINGS index_granularity = 2;

INSERT INTO t_relative_hour_pk VALUES
    ('1969-12-31 20:00:00'), ('1969-12-31 21:00:00'), ('1969-12-31 23:00:00'),
    ('1970-01-01 01:30:00'), ('1970-01-01 05:00:00'), ('1970-01-01 07:00:00');

SELECT 'PK range analysis across the epoch: indexed count must match full-scan count';
-- Pick predicates that still match rows after the fix, otherwise both arms are 0 and the check
-- passes even if key analysis prunes everything away. toRelativeHourNum clamps the three
-- pre-epoch rows to 0, and 1970-01-01 01:30:00 is the first row after the epoch, at 1.
-- force_primary_key asserts the range analysis is actually consulted rather than silently
-- falling back to a full scan that would produce the same (correct) answer.
SELECT
    (SELECT countIf(toRelativeHourNum(d) = 0) FROM t_relative_hour_pk SETTINGS force_primary_key = 0) AS full_scan_count,
    (SELECT count() FROM t_relative_hour_pk WHERE toRelativeHourNum(d) = 0 SETTINGS force_primary_key = 1) AS indexed_count,
    full_scan_count = indexed_count AS matches;
SELECT
    (SELECT countIf(toRelativeHourNum(d) = 1) FROM t_relative_hour_pk SETTINGS force_primary_key = 0) AS full_scan_count,
    (SELECT count() FROM t_relative_hour_pk WHERE toRelativeHourNum(d) = 1 SETTINGS force_primary_key = 1) AS indexed_count,
    full_scan_count = indexed_count AS matches;

DROP TABLE t_relative_hour_pk;
