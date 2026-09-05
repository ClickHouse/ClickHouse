-- `toDayOfWeek(key, mode)` is monotonic inside a Monday-based week only for the Monday-first modes 0 and 1.
-- With a Sunday-first mode the index analysis must not prune granules that hold matching rows.

DROP TABLE IF EXISTS t_dow_date;
CREATE TABLE t_dow_date (d Date) ENGINE = MergeTree ORDER BY d SETTINGS index_granularity = 1;
INSERT INTO t_dow_date SELECT toDate('2026-08-03') + number FROM numbers(14); -- two full Monday-Sunday weeks

SELECT mode, filtered, full_scan, filtered = full_scan AS same
FROM
(
    SELECT 0 AS mode, (SELECT count() FROM t_dow_date WHERE toDayOfWeek(d) >= 5) AS filtered, (SELECT countIf(toDayOfWeek(d) >= 5) FROM t_dow_date) AS full_scan
    UNION ALL
    SELECT 1, (SELECT count() FROM t_dow_date WHERE toDayOfWeek(d, 1) >= 5), (SELECT countIf(toDayOfWeek(d, 1) >= 5) FROM t_dow_date)
    UNION ALL
    SELECT 2, (SELECT count() FROM t_dow_date WHERE toDayOfWeek(d, 2) >= 5), (SELECT countIf(toDayOfWeek(d, 2) >= 5) FROM t_dow_date)
    UNION ALL
    SELECT 3, (SELECT count() FROM t_dow_date WHERE toDayOfWeek(d, 3) >= 5), (SELECT countIf(toDayOfWeek(d, 3) >= 5) FROM t_dow_date)
    UNION ALL
    -- Only the two lowest bits of the mode are significant, so 6 is the Sunday-first mode 2.
    SELECT 6, (SELECT count() FROM t_dow_date WHERE toDayOfWeek(d, 6) >= 5), (SELECT countIf(toDayOfWeek(d, 6) >= 5) FROM t_dow_date)
)
ORDER BY mode;

SELECT count() FROM t_dow_date WHERE toDayOfWeek(d, 2) = 0;
SELECT countIf(toDayOfWeek(d, 2) = 0) FROM t_dow_date;

DROP TABLE IF EXISTS t_dow_datetime;
CREATE TABLE t_dow_datetime (dt DateTime) ENGINE = MergeTree ORDER BY dt SETTINGS index_granularity = 1;
INSERT INTO t_dow_datetime SELECT toDateTime('2026-08-03 00:00:00', 'UTC') + number * 86400 FROM numbers(14);

SELECT count() FROM t_dow_datetime WHERE toDayOfWeek(dt, 3) >= 5;
SELECT countIf(toDayOfWeek(dt, 3) >= 5) FROM t_dow_datetime;

-- The Monday-first modes keep pruning granules, the Sunday-first ones read the whole table.
SELECT
    (SELECT marks FROM (EXPLAIN ESTIMATE SELECT count() FROM t_dow_date WHERE toDayOfWeek(d, 1) >= 5))
    < (SELECT marks FROM (EXPLAIN ESTIMATE SELECT count() FROM t_dow_date WHERE toDayOfWeek(d, 2) >= 5)) AS monday_mode_prunes;

DROP TABLE t_dow_date;
DROP TABLE t_dow_datetime;
