SET enable_analyzer = 1;
SET enable_materialized_cte = 1;

-- A MATERIALIZED helper CTE referenced only from the recursive member is materialized once, before the
-- recursion starts, and every recursive step reads the snapshot instead of re-evaluating the subquery.
WITH RECURSIVE evens AS MATERIALIZED
(
    SELECT number * 2 AS n FROM numbers(10)
),
search AS
(
    SELECT 0 AS x
    UNION ALL
    SELECT x + 2 FROM search WHERE x < 8 AND (x + 2) IN (SELECT n FROM evens)
)
SELECT * FROM search ORDER BY x;

-- Also works when the materialized CTE feeds a JOIN inside the recursive member.
WITH RECURSIVE seq AS MATERIALIZED
(
    SELECT number AS n FROM numbers(1, 5)
),
walk AS
(
    SELECT 1 AS x
    UNION ALL
    SELECT x + 1 FROM walk INNER JOIN seq ON seq.n = walk.x WHERE x < 5
)
SELECT * FROM walk ORDER BY x;

-- The temporary table names are random, so the plan is not pinned directly; instead count the
-- `MaterializingCTE` plan steps: exactly one materialization for a helper referenced only from the
-- recursive member.
SELECT count() FROM
(
    EXPLAIN
    WITH RECURSIVE seq AS MATERIALIZED
    (
        SELECT number AS n FROM numbers(1, 5)
    ),
    walk AS
    (
        SELECT 1 AS x
        UNION ALL
        SELECT x + 1 FROM walk INNER JOIN seq ON seq.n = walk.x WHERE x < 5
    )
    SELECT * FROM walk
) WHERE explain LIKE '%MaterializingCTE (Materializing CTE: seq)%';

-- Functional pin using nondeterminism: the snapshot is stable across recursive steps, so a
-- non-deterministic helper referenced only from the recursive member contributes exactly as many
-- distinct values as it has rows, however many steps read it.
WITH RECURSIVE snap AS MATERIALIZED
(
    SELECT rand64() AS r FROM numbers(3)
),
walk AS
(
    SELECT 1 AS x, toUInt64(0) AS v
    UNION ALL
    SELECT x + 1, snap.r FROM walk CROSS JOIN snap WHERE x < 4
)
SELECT uniqExact(v) FROM walk WHERE x > 1;

-- The same holds when the helper is referenced from both the anchor and the recursive member.
WITH RECURSIVE snap AS MATERIALIZED
(
    SELECT rand64() AS r FROM numbers(3)
),
walk AS
(
    SELECT 1 AS x, snap.r AS v FROM snap
    UNION ALL
    SELECT x + 1, snap.r FROM walk CROSS JOIN snap WHERE x < 4
)
SELECT uniqExact(v) FROM walk;

-- A materialized CTE may read the recursive CTE from the outer query.
WITH RECURSIVE walk AS
(
    SELECT 1 AS x
    UNION ALL
    SELECT x + 1 FROM walk WHERE x < 3
),
total AS MATERIALIZED
(
    SELECT sum(x) AS s FROM walk
)
SELECT * FROM total;

-- The snapshot is taken before the recursion starts, so a materialized CTE cannot read the recursive
-- CTE from inside its recursive member.
WITH RECURSIVE walk AS
(
    SELECT 1 AS x
    UNION ALL
    SELECT walk.x + 1 FROM walk INNER JOIN seen ON seen.x = walk.x WHERE walk.x < 3
),
seen AS MATERIALIZED
(
    SELECT x FROM walk
)
SELECT * FROM walk; -- { serverError UNSUPPORTED_METHOD }

-- The materialized subquery must not depend on the scope of any of its reference sites, including
-- a reference inside the recursive member where an identifier resolves to a column of the recursive CTE.
WITH RECURSIVE snap AS MATERIALIZED
(
    SELECT x < 10 AS c, rand64() AS r FROM numbers(3)
),
walk AS
(
    SELECT 1 AS x, snap.r AS v FROM snap
    UNION ALL
    SELECT x + 1, snap.r FROM walk CROSS JOIN snap WHERE x < 4
)
SELECT uniqExact(v) FROM walk; -- { serverError UNSUPPORTED_METHOD }

-- A helper CTE that is itself a UNION but does not reference itself is not recursive and may be
-- MATERIALIZED like a plain SELECT helper.
WITH RECURSIVE helper AS MATERIALIZED
(
    SELECT 1 AS n
    UNION ALL
    SELECT 2 AS n
),
search AS
(
    SELECT 1 AS x
    UNION ALL
    SELECT x + 1 FROM search WHERE x < 2 AND (x + 1) IN (SELECT n FROM helper)
)
SELECT * FROM search ORDER BY x;

-- Without `enable_materialized_cte`, MATERIALIZED is only a hint and the helper is an ordinary CTE.
SET enable_materialized_cte = 0;

WITH RECURSIVE seq AS MATERIALIZED
(
    SELECT number AS n FROM numbers(1, 5)
),
walk AS
(
    SELECT 1 AS x
    UNION ALL
    SELECT x + 1 FROM walk INNER JOIN seq ON seq.n = walk.x WHERE x < 5
)
SELECT count() FROM walk;

SET enable_materialized_cte = 1;

-- The recursive CTE itself cannot be MATERIALIZED.
WITH RECURSIVE bad AS MATERIALIZED (SELECT 1 AS x UNION ALL SELECT x + 1 FROM bad WHERE x < 3) SELECT * FROM bad; -- { serverError UNSUPPORTED_METHOD }
