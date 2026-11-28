-- A positional argument of ORDER BY or LIMIT BY referring to a window function over a named window
-- uses the definition of the window as of the projection item, not the one rewritten by a following `REPLACE`.

SET enable_analyzer = 1;
SET enable_positional_arguments = 1;

SELECT t.c, count() OVER w AS cnt, max(* REPLACE (intDiv(c, 2) AS c)) AS m
FROM (SELECT number AS c FROM numbers(4)) AS t
GROUP BY t.c WITH ROLLUP
WINDOW w AS (PARTITION BY c)
ORDER BY 2, t.c NULLS LAST, m
SETTINGS group_by_use_nulls = 0;

SELECT t.c, count() OVER w AS cnt, max(* REPLACE (intDiv(c, 2) AS c)) AS m
FROM (SELECT number AS c FROM numbers(4)) AS t
GROUP BY t.c WITH ROLLUP
WINDOW w AS (PARTITION BY c)
ORDER BY 2, t.c NULLS LAST, m
SETTINGS group_by_use_nulls = 1;

SELECT t.c, count() OVER w AS cnt, max(* REPLACE (intDiv(c, 2) AS c)) AS m
FROM (SELECT number AS c FROM numbers(4)) AS t
GROUP BY t.c WITH ROLLUP
WINDOW w AS (PARTITION BY c)
ORDER BY t.c NULLS LAST, m
LIMIT 1 BY 2
SETTINGS group_by_use_nulls = 0;

SELECT t.c, count() OVER w AS cnt, max(* REPLACE (intDiv(c, 2) AS c)) AS m
FROM (SELECT number AS c FROM numbers(4)) AS t
GROUP BY t.c WITH ROLLUP
WINDOW w AS (PARTITION BY c)
ORDER BY t.c NULLS LAST, m
LIMIT 1 BY 2
SETTINGS group_by_use_nulls = 1;
