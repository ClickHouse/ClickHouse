-- Tags: no-parallel-replicas, no-old-analyzer

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

-- The `max_rows_to_read` limits below only tell a pruned read apart from a full scan of all 400 rows.
-- They are not tight: the same row can be accounted for more than once, for example when mark ranges
-- are split into intersecting and non-intersecting ones.

SELECT 'bounds in JOIN ON';
WITH bounds AS (SELECT toDate('2025-06-01') AS lo, toDate('2025-06-10') AS hi)
SELECT count(), sum(v)
FROM t_join_const_prune AS t
JOIN bounds ON t.d >= bounds.lo AND t.d <= bounds.hi
SETTINGS max_rows_to_read = 150;

SELECT 'bounds as plain literals';
SELECT count(), sum(v)
FROM t_join_const_prune AS t
WHERE t.d >= toDate('2025-06-01') AND t.d <= toDate('2025-06-10')
SETTINGS max_rows_to_read = 150;

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

-- The bound reaches index analysis next to the equi key, so only the two 2026 partitions are read.
SELECT 'equi key plus bounds in JOIN ON';
WITH bounds AS (SELECT 395 AS k, toDate('2026-01-01') AS lo)
SELECT count(), sum(v)
FROM t_join_const_prune AS t
JOIN bounds ON t.v = bounds.k AND t.d >= bounds.lo
SETTINGS max_rows_to_read = 150;

-- A LEFT JOIN keeps every left row, so a condition on the left side cannot be applied to the left
-- input: it decides matching only. Row 395 is 2026-01-31, the single row that matches.
SELECT 'left join keeps all left rows';
WITH bounds AS (SELECT 395 AS k, toDate('2026-01-01') AS lo)
SELECT count(), countIf(lo IS NULL)
FROM t_join_const_prune AS t
LEFT JOIN bounds ON t.v = bounds.k AND t.d >= bounds.lo
SETTINGS join_use_nulls = 1;

-- A WHERE that references the right side rejects the NULL-extended rows, so the join is an INNER
-- join by the time constants are substituted. Without that rewrite the join has no equi key at all
-- and is not supported, hence `query_plan_convert_outer_join_to_inner_join` is pinned here.
SELECT 'left join with bounds in WHERE';
WITH bounds AS (SELECT toDate('2025-06-01') AS lo, toDate('2025-06-10') AS hi)
SELECT count(), sum(v)
FROM t_join_const_prune AS t
LEFT JOIN bounds ON t.d >= bounds.lo AND t.d <= bounds.hi
WHERE bounds.hi >= t.d
SETTINGS join_use_nulls = 1, query_plan_convert_outer_join_to_inner_join = 1;

-- An equality between the two sides stays a join key, it is not turned into a constant filter.
SELECT 'equality against a constant column';
WITH bounds AS (SELECT toDate('2025-06-05') AS dd)
SELECT count(), sum(v)
FROM t_join_const_prune AS t
JOIN bounds ON t.d = bounds.dd;

DROP TABLE t_join_const_prune;
