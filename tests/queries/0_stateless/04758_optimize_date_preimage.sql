-- Scope: positive preimage coverage is limited to one-argument functions on `Date` columns.
-- `Date32` sources require result-type-aware bounds and are outside this iteration.

SET enable_analyzer = 1;

DROP TABLE IF EXISTS t_optimize_date_preimage;
CREATE TABLE t_optimize_date_preimage (d Date) ENGINE = Memory;

INSERT INTO t_optimize_date_preimage VALUES
    ('2026-02-28'),
    ('2026-03-01'),
    ('2026-03-31'),
    ('2026-04-01');

SELECT 'Date month not optimized', count()
FROM t_optimize_date_preimage
WHERE toStartOfMonth(d) = toDate('2026-03-01')
SETTINGS optimize_time_filter_with_preimage = 0;

SELECT 'Date month optimized', count()
FROM t_optimize_date_preimage
WHERE toStartOfMonth(d) = toDate('2026-03-01')
SETTINGS optimize_time_filter_with_preimage = 1;

SELECT 'Date month query tree retains function', count()
FROM
(
    EXPLAIN QUERY TREE run_passes = 1
    SELECT d
    FROM t_optimize_date_preimage
    WHERE toStartOfMonth(d) = toDate('2026-03-01')
    SETTINGS optimize_time_filter_with_preimage = 1
)
WHERE explain ILIKE '%function_name: toStartOfMonth%';

SELECT 'Date day not optimized', count()
FROM t_optimize_date_preimage
WHERE toStartOfDay(d) = toDateTime('2026-03-01 00:00:00')
SETTINGS optimize_time_filter_with_preimage = 0;

SELECT 'Date day optimized', count()
FROM t_optimize_date_preimage
WHERE toStartOfDay(d) = toDateTime('2026-03-01 00:00:00')
SETTINGS optimize_time_filter_with_preimage = 1;

SELECT 'Date day query tree retains function', count()
FROM
(
    EXPLAIN QUERY TREE run_passes = 1
    SELECT d
    FROM t_optimize_date_preimage
    WHERE toStartOfDay(d) = toDateTime('2026-03-01 00:00:00')
    SETTINGS optimize_time_filter_with_preimage = 1
)
WHERE explain ILIKE '%function_name: toStartOfDay%';

DROP TABLE t_optimize_date_preimage;
