-- `grouping` function with group_by_use_nulls = 1.
-- Arguments of `grouping` only identify GROUP BY keys, so they must not be converted
-- to Nullable and must match the keys in their original form.
-- https://github.com/ClickHouse/ClickHouse/issues/77545

SET enable_analyzer = 1;
SET group_by_use_nulls = 1;

SELECT g, x, count(), grouping(g), grouping(x)
FROM (SELECT number % 3 AS g, number AS x FROM numbers(10))
GROUP BY GROUPING SETS ((g, x), (g), ())
ORDER BY g, x, grouping(g), grouping(x);

SELECT g, count(), grouping(g)
FROM (SELECT number % 3 AS g FROM numbers(10))
GROUP BY ROLLUP(g)
ORDER BY g, grouping(g);

SELECT g, x, count(), grouping(g, x)
FROM (SELECT number % 3 AS g, number % 2 AS x FROM numbers(10))
GROUP BY CUBE(g, x)
ORDER BY g, x, grouping(g, x);

-- An expression as a GROUP BY key inside `grouping`.
SELECT g + 1 AS e, count(), grouping(g + 1)
FROM (SELECT number % 3 AS g FROM numbers(10))
GROUP BY ROLLUP(g + 1)
ORDER BY e, grouping(g + 1);

-- `grouping` in HAVING and ORDER BY.
SELECT g, count()
FROM (SELECT number % 3 AS g FROM numbers(10))
GROUP BY ROLLUP(g)
HAVING grouping(g) = 0
ORDER BY g, grouping(g);

-- An argument that is not a GROUP BY key must still be an error.
SELECT g, count(), grouping(x)
FROM (SELECT number % 3 AS g, number AS x FROM numbers(10))
GROUP BY ROLLUP(g)
ORDER BY g; -- { serverError BAD_ARGUMENTS }
