-- https://github.com/ClickHouse/ClickHouse/issues/84318
-- A recursive CTE whose recursive part has more than two branches and refers to a CTE
-- that is built on top of another recursive CTE used to fail with a logical error:
-- `UNION query t1 AS (...) is not recursive`.

SET enable_analyzer = 1;

-- The query from the issue, keeping its exact shape: `subquery2` is a plain CTE over the
-- recursive `subquery1`, and the recursive part of `subquery3` has two branches, one of
-- them joining `subquery3` with `subquery2`. The original query is valid but does not
-- terminate, so a `depth` column bounds the recursion, which makes the result checkable.

WITH RECURSIVE
    subquery1 AS
    (
        SELECT 1 AS x
        UNION ALL
        SELECT x + 1
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
        SELECT
            id,
            0 AS depth
        FROM subquery2
        UNION ALL
        SELECT
            cc.id,
            cc.depth + 1
        FROM subquery3 AS cc
        INNER JOIN subquery2 AS oe ON cc.id = oe.id
        WHERE cc.depth < 2
        UNION ALL
        SELECT
            cc.id,
            cc.depth + 1
        FROM subquery3 AS cc
        WHERE cc.depth < 2
    )
SELECT
    depth,
    count()
FROM subquery3
GROUP BY depth
ORDER BY depth;

-- The same shape, but the sibling CTE propagates distinct values, so the join branch of
-- the recursive part is also constrained by the data and not only by the depth column.

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
