-- Tags: no-parallel-replicas

-- A column that is constant in a JOIN input is invisible inside the join condition and in a filter
-- above the join, because both see it as an ordinary column. Constants are substituted so that
-- predicates like `t.d >= bounds.lo` become single-sided and reach index analysis.

DROP TABLE IF EXISTS t_join_const_prune;

CREATE TABLE t_join_const_prune (d Date, v UInt32)
ENGINE = MergeTree
PARTITION BY toYYYYMM(d)
ORDER BY d;

-- 400 days starting at 2025-01-01, i.e. 14 monthly partitions; June 2025 holds 30 rows.
INSERT INTO t_join_const_prune SELECT toDate('2025-01-01') + number, number FROM numbers(400);

SELECT 'bounds in JOIN ON';
WITH bounds AS (SELECT toDate('2025-06-01') AS lo, toDate('2025-06-10') AS hi)
SELECT count(), sum(v)
FROM t_join_const_prune AS t
JOIN bounds ON t.d >= bounds.lo AND t.d <= bounds.hi
SETTINGS max_rows_to_read = 50;

-- SELECT 'bounds in WHERE over a CROSS JOIN';
-- WITH bounds AS (SELECT toDate('2025-06-01') AS lo, toDate('2025-06-10') AS hi)
-- SELECT count(), sum(v)
-- FROM t_join_const_prune AS t, bounds
-- WHERE t.d >= bounds.lo AND t.d <= bounds.hi
-- SETTINGS max_rows_to_read = 50;

SELECT 'bounds as plain literals';
SELECT count(), sum(v)
FROM t_join_const_prune AS t
WHERE t.d >= toDate('2025-06-01') AND t.d <= toDate('2025-06-10')
SETTINGS max_rows_to_read = 50;

SELECT 'pruned parts';
SET explain_query_plan_default = 'legacy';
SELECT trimLeft(explain) FROM (
    EXPLAIN indexes = 1
    WITH bounds AS (SELECT toDate('2025-06-01') AS lo, toDate('2025-06-10') AS hi)
    SELECT count()
    FROM t_join_const_prune AS t
    JOIN bounds ON t.d >= bounds.lo AND t.d <= bounds.hi
) WHERE explain LIKE '%Parts: %';
SET explain_query_plan_default = 'pretty';

SELECT 'equi key plus bounds in JOIN ON';
WITH bounds AS (SELECT 160 AS k, toDate('2025-06-01') AS lo)
SELECT count(), sum(v)
FROM t_join_const_prune AS t
JOIN bounds ON t.v = bounds.k AND t.d >= bounds.lo
SETTINGS max_rows_to_read = 300;

-- A LEFT JOIN keeps every left row, so a condition on the left side cannot be applied to the left
-- input: it decides matching only. Row 160 is 2025-06-10, the single row that matches.
SELECT 'left join keeps all left rows';
WITH bounds AS (SELECT 160 AS k, toDate('2025-06-01') AS lo)
SELECT count(), countIf(lo IS NULL)
FROM t_join_const_prune AS t
LEFT JOIN bounds ON t.v = bounds.k AND t.d >= bounds.lo
SETTINGS join_use_nulls = 1;

-- A WHERE that references the right side rejects the NULL-extended rows, so the join is an INNER
-- join by the time constants are substituted.
SELECT 'left join with bounds in WHERE';
WITH bounds AS (SELECT toDate('2025-06-01') AS lo, toDate('2025-06-10') AS hi)
SELECT count(), sum(v)
FROM t_join_const_prune AS t
LEFT JOIN bounds ON t.d >= bounds.lo AND t.d <= bounds.hi
WHERE bounds.hi >= t.d
SETTINGS join_use_nulls = 1;

-- An equality between the two sides stays a join key, it is not turned into a constant filter.
SELECT 'equality against a constant column';
WITH bounds AS (SELECT toDate('2025-06-05') AS dd)
SELECT count(), sum(v)
FROM t_join_const_prune AS t
JOIN bounds ON t.d = bounds.dd;

-- SELECT 'bounds shared by a chain of joins';
-- WITH bounds AS (SELECT toDate('2025-06-01') AS lo, toDate('2025-06-10') AS hi)
-- SELECT count(), sum(t1.v)
-- FROM t_join_const_prune AS t1
-- JOIN t_join_const_prune AS t2 ON t1.d = t2.d, bounds
-- WHERE t1.d >= bounds.lo AND t1.d <= bounds.hi
-- SETTINGS max_rows_to_read = 100;

DROP TABLE t_join_const_prune;
