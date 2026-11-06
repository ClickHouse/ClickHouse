-- With `group_by_use_nulls` and `GROUP BY ALL`, the projection is resolved once to find the grouping keys
-- and then resolved again. A window function referencing a named window must keep the definition as of
-- the time it is reached in the projection, even if a following `WINDOW` entry rewrites it with `REPLACE`.
-- Every query is run at `group_by_use_nulls = 0` and at `group_by_use_nulls = 1`, and the two results
-- have to be the same, except for the NULL key of the total row.

SET enable_analyzer = 1;

SELECT c, count() OVER w AS cnt
FROM (SELECT number AS c FROM numbers(4)) AS t
GROUP BY ALL WITH ROLLUP
WINDOW w2 AS (PARTITION BY * REPLACE (intDiv(c, 2) AS c)),
       w AS (PARTITION BY c)
ORDER BY t.c NULLS LAST, cnt
SETTINGS group_by_use_nulls = 0;

SELECT c, count() OVER w AS cnt
FROM (SELECT number AS c FROM numbers(4)) AS t
GROUP BY ALL WITH ROLLUP
WINDOW w2 AS (PARTITION BY * REPLACE (intDiv(c, 2) AS c)),
       w AS (PARTITION BY c)
ORDER BY t.c NULLS LAST, cnt
SETTINGS group_by_use_nulls = 1;

SELECT c, count() OVER (w ORDER BY c) AS cnt
FROM (SELECT number AS c FROM numbers(4)) AS t
GROUP BY ALL WITH ROLLUP
WINDOW w2 AS (PARTITION BY * REPLACE (intDiv(c, 2) AS c)),
       w AS (PARTITION BY c)
ORDER BY t.c NULLS LAST, cnt
SETTINGS group_by_use_nulls = 0;

SELECT c, count() OVER (w ORDER BY c) AS cnt
FROM (SELECT number AS c FROM numbers(4)) AS t
GROUP BY ALL WITH ROLLUP
WINDOW w2 AS (PARTITION BY * REPLACE (intDiv(c, 2) AS c)),
       w AS (PARTITION BY c)
ORDER BY t.c NULLS LAST, cnt
SETTINGS group_by_use_nulls = 1;

-- The same without `GROUP BY ALL`: a matcher in a clause resolved before the projection must not rewrite it.

SELECT c, count() OVER w AS cnt
FROM (SELECT number AS c FROM numbers(4)) AS t
GROUP BY c WITH ROLLUP
WINDOW w2 AS (PARTITION BY * REPLACE (intDiv(c, 2) AS c)),
       w AS (PARTITION BY c)
ORDER BY t.c NULLS LAST, cnt
SETTINGS group_by_use_nulls = 0;

SELECT c, count() OVER w AS cnt
FROM (SELECT number AS c FROM numbers(4)) AS t
GROUP BY c WITH ROLLUP
WINDOW w2 AS (PARTITION BY * REPLACE (intDiv(c, 2) AS c)),
       w AS (PARTITION BY c)
ORDER BY t.c NULLS LAST, cnt
SETTINGS group_by_use_nulls = 1;

SELECT c
FROM (SELECT number AS c FROM numbers(4)) AS t
GROUP BY c WITH ROLLUP
HAVING max(* REPLACE (intDiv(c, 2) AS c)) >= 0
ORDER BY t.c NULLS LAST
SETTINGS group_by_use_nulls = 0;

SELECT c
FROM (SELECT number AS c FROM numbers(4)) AS t
GROUP BY c WITH ROLLUP
HAVING max(* REPLACE (intDiv(c, 2) AS c)) >= 0
ORDER BY t.c NULLS LAST
SETTINGS group_by_use_nulls = 1;
