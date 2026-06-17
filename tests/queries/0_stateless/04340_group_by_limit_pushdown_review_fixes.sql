-- Regression tests for review fixes to the GROUP BY top-K optimization.

SET enable_group_by_top_k_optimization = 1;
-- CI randomizes query_plan_max_limit_for_top_k_optimization (can be tiny), which would
-- gate the optimization off for the limits used here; pin it.
SET query_plan_max_limit_for_top_k_optimization = 1000;
SET max_threads = 1;
-- The CI test profile sets max_rows_to_group_by, which disables the optimization; reset it.
SET max_rows_to_group_by = 0;
SET optimize_trivial_group_by_limit_query = 0;

-- ---------------------------------------------------------------------------
-- Collated ORDER BY is kept out of the optimization: the top-K heap compares
-- with IColumn::compareAt, which ignores collation, so the optimizer bails on
-- any collated ORDER BY column.  The plan must not carry Top-K, and the result
-- must still be correct.
-- ---------------------------------------------------------------------------
SELECT 'collator: no Top-K in plan';
SELECT count() FROM (EXPLAIN actions = 1
    SELECT s FROM (SELECT toString(number % 100) AS s FROM numbers(1000)) GROUP BY s ORDER BY s ASC COLLATE 'en' LIMIT 5
    SETTINGS max_bytes_before_external_group_by = 0, max_bytes_ratio_before_external_group_by = 0
) WHERE explain LIKE '%Top-K%';

SELECT 'collator: result matches optimization off';
SELECT count() FROM (
    SELECT s FROM (SELECT toString(number % 100) AS s FROM numbers(10000)) GROUP BY s ORDER BY s ASC COLLATE 'en' LIMIT 5
    SETTINGS enable_group_by_top_k_optimization = 1
) AS l
INNER JOIN (
    SELECT s FROM (SELECT toString(number % 100) AS s FROM numbers(10000)) GROUP BY s ORDER BY s ASC COLLATE 'en' LIMIT 5
    SETTINGS enable_group_by_top_k_optimization = 0
) AS r USING (s);

-- ---------------------------------------------------------------------------
-- Pattern 2 (GROUP BY ... LIMIT without ORDER BY) stays enabled even when
-- external aggregation is configured: the heap bounds the table so a spill
-- never triggers, and applyLimitPushdown turns external aggregation off for the
-- step (removing the spilled-then-evicted incomplete-group hazard).  The plan
-- must still carry Top-K.
-- ---------------------------------------------------------------------------
SELECT 'external agg: Pattern 2 still applies';
SELECT count() FROM (EXPLAIN actions = 1
    SELECT k FROM (SELECT number % 100 AS k FROM numbers(1000)) GROUP BY k LIMIT 5
    SETTINGS max_bytes_ratio_before_external_group_by = 0.5
) WHERE explain LIKE '%Top-K%';

SELECT 'external agg: Pattern 1 still applies';
SELECT count() FROM (EXPLAIN actions = 1
    SELECT k FROM (SELECT number % 100 AS k FROM numbers(1000)) GROUP BY k ORDER BY k LIMIT 5
    SETTINGS max_bytes_ratio_before_external_group_by = 0.5
) WHERE explain LIKE '%Top-K%';

-- ---------------------------------------------------------------------------
-- A limit too large to be selective is not worth the heap overhead (it never
-- reaches capacity, so it can neither skip nor freeze, yet copies every key and
-- maintains the heap).  query_plan_max_limit_for_top_k_optimization (default
-- 1000) caps it: a limit above the cap must not carry Top-K, at or below it must.
-- ---------------------------------------------------------------------------
SELECT 'huge limit: gated off';
SELECT count() FROM (EXPLAIN actions = 1
    SELECT k FROM (SELECT number % 100 AS k FROM numbers(1000)) GROUP BY k ORDER BY k LIMIT 1000000000
    SETTINGS max_bytes_ratio_before_external_group_by = 0
) WHERE explain LIKE '%Top-K%';

SELECT 'limit at cap: applies';
SELECT count() FROM (EXPLAIN actions = 1
    SELECT k FROM (SELECT number % 100 AS k FROM numbers(1000)) GROUP BY k ORDER BY k LIMIT 1000
    SETTINGS query_plan_max_limit_for_top_k_optimization = 1000, max_bytes_ratio_before_external_group_by = 0
) WHERE explain LIKE '%Top-K%';

SELECT 'limit above cap: gated off';
SELECT count() FROM (EXPLAIN actions = 1
    SELECT k FROM (SELECT number % 100 AS k FROM numbers(1000)) GROUP BY k ORDER BY k LIMIT 1001
    SETTINGS query_plan_max_limit_for_top_k_optimization = 1000, max_bytes_ratio_before_external_group_by = 0
) WHERE explain LIKE '%Top-K%';

-- ---------------------------------------------------------------------------
-- Nullable keys are erased from the hash table on eviction (not silently left
-- behind via the null-slot early return), so an evicted-then-readmitted null
-- group does not surface with an incomplete aggregate.  Compare against
-- optimization off under heavy eviction churn with NULLs in the stream.
-- ---------------------------------------------------------------------------
SELECT 'nullable key eviction: result matches optimization off';
SELECT count(), countIf(same) FROM (
    SELECT l.c = r.c AND l.s = r.s AS same FROM (
        SELECT k, count() AS c, sum(v) AS s FROM (
            SELECT if(number % 500 = 0, NULL, toNullable(toString(999999 - number))) AS k, number AS v FROM numbers(200000)
        ) GROUP BY k ORDER BY k ASC NULLS FIRST LIMIT 10
        SETTINGS enable_group_by_top_k_optimization = 1, max_bytes_before_external_group_by = 0, max_bytes_ratio_before_external_group_by = 0
    ) AS l
    INNER JOIN (
        SELECT k, count() AS c, sum(v) AS s FROM (
            SELECT if(number % 500 = 0, NULL, toNullable(toString(999999 - number))) AS k, number AS v FROM numbers(200000)
        ) GROUP BY k ORDER BY k ASC NULLS FIRST LIMIT 10
        SETTINGS enable_group_by_top_k_optimization = 0
    ) AS r ON l.k IS NOT DISTINCT FROM r.k
);
