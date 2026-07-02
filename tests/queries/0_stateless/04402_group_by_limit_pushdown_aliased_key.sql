-- Tags: no-old-analyzer
-- Applicability and correctness of the GROUP BY top-K optimization when a projection
-- reuses a GROUP BY key's name for a different expression (e.g. `-k AS k` with
-- `prefer_column_name_to_alias`).  These cases hinge on how `ORDER BY k` is resolved
-- against the colliding name, which is only well-defined under the analyzer; the old
-- analyzer resolves the same construct inconsistently across query contexts.

SET enable_group_by_top_k_optimization = 1;
SET query_plan_max_limit_for_top_k_optimization = 1000;
SET max_threads = 1;
SET max_rows_to_group_by = 0;
SET optimize_trivial_group_by_limit_query = 0;

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
