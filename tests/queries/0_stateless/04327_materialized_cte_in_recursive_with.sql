SET enable_analyzer = 1;

-- A MATERIALIZED helper CTE referenced from a recursive member: materialized once,
-- the recursive steps read the snapshot instead of re-evaluating the subquery per step.
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

-- The recursive CTE itself cannot be MATERIALIZED.
WITH RECURSIVE bad AS MATERIALIZED (SELECT 1 AS x UNION ALL SELECT x + 1 FROM bad WHERE x < 3) SELECT * FROM bad; -- { serverError UNSUPPORTED_METHOD }
