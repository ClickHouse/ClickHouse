-- Gating and applicability of the GROUP BY top-K optimization: when it stays on
-- (external aggregation), when it is kept out (non-binding limit, non-prefix
-- ORDER BY), and correctness at the edges (nullable key eviction).

SET enable_group_by_top_k_optimization = 1;
SET query_plan_max_limit_for_top_k_optimization = 1000;
SET max_threads = 1;
SET max_rows_to_group_by = 0;
SET optimize_trivial_group_by_limit_query = 0;

-- Pattern 2 stays enabled under external aggregation: the heap bounds the table so a
-- spill never triggers, and pushdown turns external aggregation off for the step.
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

-- A limit too large to be selective is not worth the heap overhead;
-- `query_plan_max_limit_for_top_k_optimization` (default 1000) caps it.
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

-- Nullable keys are erased from the hash table on eviction, so an evicted-then-readmitted
-- null group does not surface with an incomplete aggregate.
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

-- ORDER BY that is not a leading prefix of the GROUP BY keys is left unoptimized:
-- the optimizer's positional gate bails. No Top-K in the plan, results stay correct.
SELECT 'non-prefix ORDER BY: not optimized';
SELECT count() FROM (EXPLAIN actions = 1
    SELECT a, b FROM (SELECT number % 10 AS a, number % 7 AS b FROM numbers(1000)) GROUP BY a, b ORDER BY b LIMIT 5
) WHERE explain LIKE '%Top-K%';

SELECT 'reordered ORDER BY: not optimized';
SELECT count() FROM (EXPLAIN actions = 1
    SELECT a, b FROM (SELECT number % 10 AS a, number % 7 AS b FROM numbers(1000)) GROUP BY a, b ORDER BY b, a LIMIT 5
) WHERE explain LIKE '%Top-K%';

SELECT 'non-prefix ORDER BY: result matches optimization off';
SELECT count() FROM (
    SELECT a, b FROM (SELECT number % 10 AS a, number % 7 AS b FROM numbers(100000)) GROUP BY a, b ORDER BY b, a LIMIT 5
    SETTINGS enable_group_by_top_k_optimization = 1
) AS l
INNER JOIN (
    SELECT a, b FROM (SELECT number % 10 AS a, number % 7 AS b FROM numbers(100000)) GROUP BY a, b ORDER BY b, a LIMIT 5
    SETTINGS enable_group_by_top_k_optimization = 0
) AS r USING (a, b);
