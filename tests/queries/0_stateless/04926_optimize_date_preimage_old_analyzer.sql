-- Scope: legacy-planner coverage for constant expressions used with one-argument preimage
-- functions on `Date` and `DateTime` columns. Extended date types and explicit-timezone
-- overloads are outside this iteration.

SET enable_analyzer = 0;
SET optimize_time_filter_with_preimage = 1;

DROP TABLE IF EXISTS t_optimize_date_preimage_old_analyzer;
CREATE TABLE t_optimize_date_preimage_old_analyzer
(
    ts DateTime('UTC'),
    d Date
)
ENGINE = Memory;

INSERT INTO t_optimize_date_preimage_old_analyzer VALUES
    ('2026-02-28 23:59:59', '2026-02-28'),
    ('2026-03-01 00:00:00', '2026-03-01'),
    ('2026-03-08 00:00:00', '2026-03-08'),
    ('2026-03-08 23:59:59', '2026-03-08'),
    ('2026-03-09 00:00:00', '2026-03-09'),
    ('2026-04-01 00:00:00', '2026-04-01');

SELECT 'toDate result', count()
FROM t_optimize_date_preimage_old_analyzer
WHERE toDate(ts) = toDate('2026-03-08');

SELECT 'toDate retained', count()
FROM
(
    EXPLAIN SYNTAX oneline = 1
    SELECT ts
    FROM t_optimize_date_preimage_old_analyzer
    WHERE toDate(ts) = toDate('2026-03-08')
)
WHERE explain ILIKE '%toDate(ts)%';

SELECT 'today retains toDate', count()
FROM
(
    EXPLAIN SYNTAX oneline = 1
    SELECT ts
    FROM t_optimize_date_preimage_old_analyzer
    WHERE toDate(ts) = today()
)
WHERE explain ILIKE '%toDate(ts)%';

SELECT 'nonconstant retains toDate', count()
FROM
(
    EXPLAIN SYNTAX oneline = 1
    SELECT ts
    FROM t_optimize_date_preimage_old_analyzer
    WHERE toDate(ts) = toDate(nowInBlock())
)
WHERE explain ILIKE '%toDate(ts)%';

SELECT 'toStartOfMonth result', count()
FROM t_optimize_date_preimage_old_analyzer
WHERE toStartOfMonth(ts) = toDate('2026-03-01');

SELECT 'toStartOfMonth retained', count()
FROM
(
    EXPLAIN SYNTAX oneline = 1
    SELECT ts
    FROM t_optimize_date_preimage_old_analyzer
    WHERE toStartOfMonth(ts) = toDate('2026-03-01')
)
WHERE explain ILIKE '%toStartOfMonth(ts)%';

SELECT 'Date toStartOfDay result', count()
FROM t_optimize_date_preimage_old_analyzer
WHERE toStartOfDay(d) = toDateTime('2026-03-01 00:00:00');

SELECT 'Date toStartOfDay retained', count()
FROM
(
    EXPLAIN SYNTAX oneline = 1
    SELECT d
    FROM t_optimize_date_preimage_old_analyzer
    WHERE toStartOfDay(d) = toDateTime('2026-03-01 00:00:00')
)
WHERE explain ILIKE '%toStartOfDay(d)%';

SELECT 'reversed comparison result', count()
FROM t_optimize_date_preimage_old_analyzer
WHERE toDate('2026-03-08') < toDate(ts);

SELECT 'reversed comparison retains toDate', count()
FROM
(
    EXPLAIN SYNTAX oneline = 1
    SELECT ts
    FROM t_optimize_date_preimage_old_analyzer
    WHERE toDate('2026-03-08') < toDate(ts)
)
WHERE explain ILIKE '%toDate(ts)%';

DROP TABLE t_optimize_date_preimage_old_analyzer;
