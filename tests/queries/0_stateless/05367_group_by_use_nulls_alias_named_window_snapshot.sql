-- With `group_by_use_nulls`, an alias of a window function over a named window referred to from `QUALIFY`, `ORDER BY`
-- or `LIMIT BY` uses the definition of the window as of the projection item, not the one rewritten by a following `REPLACE`.

SET enable_analyzer = 1;

SELECT t.c, count() OVER w AS cnt, max(* REPLACE (intDiv(c, 2) AS c)) AS m
FROM (SELECT number AS c FROM numbers(4)) AS t
GROUP BY t.c WITH ROLLUP
WINDOW w AS (PARTITION BY c)
QUALIFY cnt = 1
ORDER BY t.c NULLS LAST
SETTINGS group_by_use_nulls = 1;

SELECT '--';

SELECT t.c, row_number() OVER w AS rn, max(* REPLACE (intDiv(c, 2) AS c)) AS m
FROM (SELECT number AS c FROM numbers(4)) AS t
GROUP BY t.c WITH ROLLUP
WINDOW w AS (PARTITION BY c ORDER BY c)
ORDER BY rn, t.c NULLS LAST
SETTINGS group_by_use_nulls = 1;

SELECT '--';

SELECT t.c, count() OVER w AS cnt, max(* REPLACE (intDiv(c, 2) AS c)) AS m
FROM (SELECT number AS c FROM numbers(4)) AS t
GROUP BY t.c WITH ROLLUP
WINDOW w AS (PARTITION BY c)
ORDER BY t.c NULLS LAST
LIMIT 1 BY cnt
SETTINGS group_by_use_nulls = 1;
