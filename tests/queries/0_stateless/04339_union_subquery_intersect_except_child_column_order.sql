-- Outer query needs a subset of columns of a UNION whose children are INTERSECT/EXCEPT.
-- The old analyzer used to abort with LOGICAL_ERROR "Different order of columns in UNION subquery"
-- because INTERSECT/EXCEPT children ignore required_result_column_names and return the full header.

SET enable_analyzer = 0;

SELECT x FROM
(
    SELECT 0 AS x, 1 AS y INTERSECT DISTINCT SELECT 0 AS x, 1 AS y
    UNION ALL
    SELECT 0 AS x, 1 AS y INTERSECT DISTINCT SELECT 0 AS x, 1 AS y
)
ORDER BY x;

SELECT x FROM
(
    SELECT 0 AS x, 1 AS y INTERSECT SELECT 0 AS x, 1 AS y
    UNION ALL
    SELECT 0 AS x, 1 AS y INTERSECT SELECT 0 AS x, 1 AS y
)
ORDER BY x;

SELECT a FROM
(
    (SELECT number AS a, number * 10 AS b FROM numbers(5) INTERSECT SELECT number AS a, number * 10 AS b FROM numbers(3))
    UNION ALL
    (SELECT number AS a, number * 10 AS b FROM numbers(2))
)
ORDER BY a;

SELECT a FROM
(
    (SELECT number AS a, number AS b FROM numbers(5) EXCEPT SELECT number AS a, number AS b FROM numbers(2))
    UNION ALL
    (SELECT 100 AS a, 100 AS b)
)
ORDER BY a;
