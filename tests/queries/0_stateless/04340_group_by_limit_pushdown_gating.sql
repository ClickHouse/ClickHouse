-- Gating and applicability of the GROUP BY top-K optimization, and correctness at the edges.

SET enable_group_by_top_k_optimization = 1;
SET query_plan_max_limit_for_top_k_optimization = 1000;
SET max_threads = 1;
SET max_rows_to_group_by = 0;
SET optimize_trivial_group_by_limit_query = 0;

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

SELECT 'nullable key eviction: result matches optimization off';
SELECT count(), countIf(same) FROM (
    SELECT l.c = r.c AND l.s = r.s AS same FROM (
        SELECT k, count() AS c, sum(v) AS s FROM (
            SELECT if(number % 500 = 0, NULL, toNullable(toString(999999 - number))) AS k, number AS v FROM numbers(40000)
        ) GROUP BY k ORDER BY k ASC NULLS FIRST LIMIT 10
        SETTINGS enable_group_by_top_k_optimization = 1, max_bytes_before_external_group_by = 0, max_bytes_ratio_before_external_group_by = 0, max_block_size = 4096
    ) AS l
    INNER JOIN (
        SELECT k, count() AS c, sum(v) AS s FROM (
            SELECT if(number % 500 = 0, NULL, toNullable(toString(999999 - number))) AS k, number AS v FROM numbers(40000)
        ) GROUP BY k ORDER BY k ASC NULLS FIRST LIMIT 10
        SETTINGS enable_group_by_top_k_optimization = 0
    ) AS r ON l.k IS NOT DISTINCT FROM r.k
);

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
    SELECT a, b FROM (SELECT number % 10 AS a, number % 7 AS b FROM numbers(20000)) GROUP BY a, b ORDER BY b, a LIMIT 5
    SETTINGS enable_group_by_top_k_optimization = 1
) AS l
INNER JOIN (
    SELECT a, b FROM (SELECT number % 10 AS a, number % 7 AS b FROM numbers(20000)) GROUP BY a, b ORDER BY b, a LIMIT 5
    SETTINGS enable_group_by_top_k_optimization = 0
) AS r USING (a, b);

-- A projection between the sort and the aggregation may reuse a GROUP BY key's
-- name for a different expression (e.g. `-k AS k` with `prefer_column_name_to_alias`).
-- The heap ranks by the actual GROUP BY key, so the optimizer only matches an
-- ORDER BY key against a GROUP BY key when that key passes through the projection
-- unchanged.  A plain `ORDER BY <key>` still passes through (the user rename lives
-- in the final projection after LIMIT), so it must keep optimizing; a non-pass-through
-- must never produce a wrong top-N.
SELECT 'aliased key passes through: still optimized';
SELECT count() FROM (EXPLAIN actions = 1
    SELECT -k AS k, count() FROM (SELECT number % 1000 AS k FROM numbers(1000)) GROUP BY k ORDER BY k ASC LIMIT 5
    SETTINGS prefer_column_name_to_alias = 1
) WHERE explain LIKE '%Top-K%';

SELECT 'aliased key, prefer_column_name_to_alias = 1: result matches optimization off';
SELECT count() FROM (
    SELECT -k AS k, count() AS c FROM (SELECT number % 1000 AS k FROM numbers(20000)) GROUP BY k ORDER BY k ASC LIMIT 5
    SETTINGS prefer_column_name_to_alias = 1, enable_group_by_top_k_optimization = 1
) AS l
INNER JOIN (
    SELECT -k AS k, count() AS c FROM (SELECT number % 1000 AS k FROM numbers(20000)) GROUP BY k ORDER BY k ASC LIMIT 5
    SETTINGS prefer_column_name_to_alias = 1, enable_group_by_top_k_optimization = 0
) AS r USING (k, c);

SELECT 'aliased key, ORDER BY position: result matches optimization off';
SELECT count() FROM (
    SELECT -k AS k, count() AS c FROM (SELECT number % 1000 AS k FROM numbers(20000)) GROUP BY k ORDER BY 1 ASC LIMIT 5
    SETTINGS prefer_column_name_to_alias = 1, enable_group_by_top_k_optimization = 1
) AS l
INNER JOIN (
    SELECT -k AS k, count() AS c FROM (SELECT number % 1000 AS k FROM numbers(20000)) GROUP BY k ORDER BY 1 ASC LIMIT 5
    SETTINGS prefer_column_name_to_alias = 1, enable_group_by_top_k_optimization = 0
) AS r USING (k, c);
