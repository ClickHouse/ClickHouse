-- https://github.com/ClickHouse/ClickHouse/issues/84318
-- A recursive CTE whose recursive part has more than two branches and refers to a CTE
-- that is built on top of another recursive CTE used to fail with a logical error:
-- `UNION query t1 AS (...) is not recursive`.

SET enable_analyzer = 1;

WITH RECURSIVE
    t1 AS
    (
        SELECT 1 AS x
        UNION ALL
        SELECT x + 1
        FROM t1
        WHERE x < 5
    ),
    t2 AS
    (
        SELECT x AS id
        FROM t1
    ),
    t3 AS
    (
        SELECT id
        FROM t2
        UNION ALL
        SELECT cc.id + 1
        FROM t3 AS cc
        INNER JOIN t2 AS oe ON cc.id = oe.id
        WHERE cc.id < 3
        UNION ALL
        SELECT cc.id + 1
        FROM t3 AS cc
        WHERE cc.id < 3
    )
SELECT
    sum(id),
    count()
FROM t3;

-- The exact query from the issue. It is a valid, but non-terminating query,
-- so the recursion depth is limited to keep the test fast.

SET max_recursive_cte_evaluation_depth = 5;

WITH RECURSIVE
    subquery1 AS
    (
        SELECT 1 AS x
        UNION ALL
        SELECT x + 1 AS level
        FROM subquery1
        WHERE x < 5
    ),
    subquery2 AS
    (
        SELECT 1 AS id
        FROM subquery1
    ),
    subquery3 AS
    (
        SELECT id
        FROM subquery2
        UNION ALL
        SELECT cc.id
        FROM subquery3 AS cc
        INNER JOIN subquery2 AS oe ON cc.id = oe.id
        UNION ALL
        SELECT cc.id
        FROM subquery3 AS cc
    )
SELECT *
FROM subquery3
FORMAT Null; -- { serverError TOO_DEEP_RECURSION }
