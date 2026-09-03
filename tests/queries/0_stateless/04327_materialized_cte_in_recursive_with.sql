SET enable_analyzer = 1;
SET enable_materialized_cte = 1;

-- A MATERIALIZED helper CTE referenced from a recursive member.
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

-- A materialized helper referenced from the non-recursive (anchor) member is evaluated once and
-- every recursive step reads the same snapshot, so a non-deterministic subquery contributes
-- exactly as many distinct values as it has rows.
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

-- A materialized CTE may read the recursive CTE.
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

-- The recursive CTE itself cannot be MATERIALIZED.
WITH RECURSIVE bad AS MATERIALIZED (SELECT 1 AS x UNION ALL SELECT x + 1 FROM bad WHERE x < 3) SELECT * FROM bad; -- { serverError UNSUPPORTED_METHOD }
