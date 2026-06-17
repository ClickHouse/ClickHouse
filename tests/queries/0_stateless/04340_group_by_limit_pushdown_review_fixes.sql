-- Regression tests for review fixes to the GROUP BY top-K optimization.

SET enable_group_by_top_k_optimization = 1;
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
-- Pattern 2 (GROUP BY ... LIMIT without ORDER BY) is disabled when external
-- aggregation can spill: a spilled-then-evicted key would surface an incomplete
-- group in the unsorted LIMIT.  With spilling possible, the plan must not carry
-- Top-K; results stay correct either way.
-- ---------------------------------------------------------------------------
SELECT 'external agg: Pattern 2 gated off';
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
