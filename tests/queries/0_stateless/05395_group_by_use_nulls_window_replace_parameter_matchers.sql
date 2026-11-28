-- With `group_by_use_nulls`, the matchers of the projection are expanded before the other clauses, as without the setting.
-- Each query is run with both values of the setting, and the results must match: there is no empty grouping set.

SET enable_analyzer = 1;

-- A `REPLACE` matcher in `WHERE` rewrites a named window consumed by a later clause, even though the `WINDOW` clause
-- is restored before the second expansion of the projection matchers.
SELECT t.*
FROM (SELECT number AS c FROM numbers(3)) AS t
WHERE identity(* REPLACE (intDiv(c, 2) AS c)) >= 0
GROUP BY GROUPING SETS ((t.c))
WINDOW w AS (PARTITION BY c)
ORDER BY count() OVER w, t.c
LIMIT 1
SETTINGS group_by_use_nulls = 0;

SELECT t.*
FROM (SELECT number AS c FROM numbers(3)) AS t
WHERE identity(* REPLACE (intDiv(c, 2) AS c)) >= 0
GROUP BY GROUPING SETS ((t.c))
WINDOW w AS (PARTITION BY c)
ORDER BY count() OVER w, t.c
LIMIT 1
SETTINGS group_by_use_nulls = 1;

-- The same with the matcher in `GROUP BY`.
SELECT t.*
FROM (SELECT number AS c FROM numbers(3)) AS t
GROUP BY GROUPING SETS ((t.c, identity(* REPLACE (intDiv(c, 2) AS c))))
WINDOW w AS (PARTITION BY c)
ORDER BY count() OVER w, t.c
LIMIT 1
SETTINGS group_by_use_nulls = 0;

SELECT t.*
FROM (SELECT number AS c FROM numbers(3)) AS t
GROUP BY GROUPING SETS ((t.c, identity(* REPLACE (intDiv(c, 2) AS c))))
WINDOW w AS (PARTITION BY c)
ORDER BY count() OVER w, t.c
LIMIT 1
SETTINGS group_by_use_nulls = 1;

-- A window function of the projection still takes the named window without the rewrite of `WHERE`.
SELECT t.c, count() OVER w
FROM (SELECT number AS c FROM numbers(3)) AS t
WHERE identity(* REPLACE (intDiv(c, 2) AS c)) >= 0
GROUP BY GROUPING SETS ((t.c))
WINDOW w AS (PARTITION BY c)
ORDER BY t.c
SETTINGS group_by_use_nulls = 0;

SELECT t.c, count() OVER w
FROM (SELECT number AS c FROM numbers(3)) AS t
WHERE identity(* REPLACE (intDiv(c, 2) AS c)) >= 0
GROUP BY GROUPING SETS ((t.c))
WINDOW w AS (PARTITION BY c)
ORDER BY t.c
SETTINGS group_by_use_nulls = 1;

-- A matcher in the parameters of an aggregate function rewrites the sibling clauses.
SELECT quantile(toFloat64(* REPLACE (0.5 AS c)))(t.c)
FROM (SELECT number AS c FROM numbers(3)) AS t
GROUP BY GROUPING SETS ((t.c))
HAVING c = 0.5
ORDER BY ALL
SETTINGS group_by_use_nulls = 0;

SELECT quantile(toFloat64(* REPLACE (0.5 AS c)))(t.c)
FROM (SELECT number AS c FROM numbers(3)) AS t
GROUP BY GROUPING SETS ((t.c))
HAVING c = 0.5
ORDER BY ALL
SETTINGS group_by_use_nulls = 1;

SELECT quantile(toFloat64(* REPLACE (0.5 AS c)))(t.c)
FROM (SELECT number AS c FROM numbers(3)) AS t
WHERE c = 0.5
GROUP BY GROUPING SETS ((t.c))
ORDER BY ALL
SETTINGS group_by_use_nulls = 0;

SELECT quantile(toFloat64(* REPLACE (0.5 AS c)))(t.c)
FROM (SELECT number AS c FROM numbers(3)) AS t
WHERE c = 0.5
GROUP BY GROUPING SETS ((t.c))
ORDER BY ALL
SETTINGS group_by_use_nulls = 1;
