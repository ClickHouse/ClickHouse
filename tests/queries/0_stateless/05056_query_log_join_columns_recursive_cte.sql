-- Tags: no-old-analyzer

-- `used_number_of_joins` and the arrays that describe the joins report the joins of the query, and the
-- recursive member of a recursive CTE has its pipeline built once per iteration, all of these builds
-- reporting into the counters of the same query because they share one context. A join of the recursive
-- member must be counted once all the same, so that the counters describe the shape of the query and do
-- not grow with the recursion depth.
--
-- The non-recursive member is a different subquery that is built exactly once, so its own joins are
-- counted next to the ones of the recursive member instead of being deduplicated together with them.
--
-- The name of a CTE is only an alias, and one query may hold several independent `WITH RECURSIVE` that
-- chose the same alias. Those are different joins and are counted apart, so the deduplication has to
-- name the CTE instance and not the alias.
--
-- The recursion depth is fixed by the `WHERE`, so the number of iterations does not depend on the
-- machine, and the algorithm is set explicitly, because the choice among the algorithms allowed by
-- `join_algorithm` is made at run time and depends on the number of threads.

SET log_queries = 1;
-- The reported kind is the executed one, and the optimizer may execute a join with its sides swapped,
-- which reverses LEFT and RIGHT.
SET query_plan_join_swap_table = 0;

CREATE TABLE dim (a UInt64) ENGINE = MergeTree ORDER BY a;
INSERT INTO dim SELECT number FROM numbers(100);

SELECT 'one join in the recursive member, five iterations';
WITH RECURSIVE r AS (
    SELECT toUInt64(1) AS n
    UNION ALL
    SELECT r.n + 1 AS n FROM r JOIN dim ON r.n = dim.a WHERE r.n < 5
)
SELECT count() FROM r
SETTINGS log_comment = '05056_recursive_cte_a_recursive_member', join_algorithm = 'hash';

SELECT 'one join in each member of the recursive CTE';
WITH RECURSIVE r AS (
    SELECT dim.a AS n FROM dim JOIN dim AS d2 ON dim.a = d2.a WHERE dim.a = 1
    UNION ALL
    SELECT r.n + 1 AS n FROM r JOIN dim ON r.n = dim.a WHERE r.n < 5
)
SELECT count() FROM r
SETTINGS log_comment = '05056_recursive_cte_b_both_members', join_algorithm = 'hash';

SELECT 'two independent recursive CTEs that share the name r';
SELECT
    (
        WITH RECURSIVE r AS (
            SELECT toUInt64(1) AS n
            UNION ALL
            SELECT r.n + 1 AS n FROM r JOIN dim ON r.n = dim.a WHERE r.n < 5
        )
        SELECT count() FROM r
    ) AS first_cte,
    (
        WITH RECURSIVE r AS (
            SELECT toUInt64(1) AS n
            UNION ALL
            SELECT r.n + 1 AS n FROM r JOIN dim ON r.n = dim.a WHERE r.n < 5
        )
        SELECT count() FROM r
    ) AS second_cte
SETTINGS log_comment = '05056_recursive_cte_c_two_same_named', join_algorithm = 'hash';

SYSTEM FLUSH LOGS query_log;
SELECT log_comment, used_number_of_joins, used_join_algorithms, used_join_kinds, used_join_strictness
FROM system.query_log
WHERE current_database = currentDatabase()
  AND type = 'QueryFinish'
  AND event_date >= yesterday()
  AND log_comment LIKE '05056\_recursive\_cte\_%'
ORDER BY log_comment;

DROP TABLE dim;
