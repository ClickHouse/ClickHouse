-- The `REPLACE` transformer of a projection matcher rewrites the named windows in the `WINDOW` clause,
-- but a window function preceding the matcher in the projection uses the definition as it was before.
-- Under `group_by_use_nulls` the matchers are expanded in advance, and the preceding window functions
-- must not see the rewritten definition either. Every query below is run at `group_by_use_nulls = 0`
-- and at `group_by_use_nulls = 1`, and the two results have to be the same, except for the NULL key of the total row.

SET enable_analyzer = 1;

SELECT '-- OVER w';
SELECT t.c, count() OVER w AS cnt, max(* REPLACE (intDiv(c, 2) AS c)) AS m
FROM (SELECT number AS c FROM numbers(4)) AS t
GROUP BY t.c WITH ROLLUP
WINDOW w AS (PARTITION BY c)
ORDER BY t.c NULLS LAST, cnt, m
SETTINGS group_by_use_nulls = 0;
SELECT t.c, count() OVER w AS cnt, max(* REPLACE (intDiv(c, 2) AS c)) AS m
FROM (SELECT number AS c FROM numbers(4)) AS t
GROUP BY t.c WITH ROLLUP
WINDOW w AS (PARTITION BY c)
ORDER BY t.c NULLS LAST, cnt, m
SETTINGS group_by_use_nulls = 1;

SELECT '-- OVER (w ORDER BY ...)';
SELECT t.c, count() OVER (w ORDER BY t.c, count() ROWS UNBOUNDED PRECEDING) AS cnt, max(* REPLACE (intDiv(c, 2) AS c)) AS m
FROM (SELECT number AS c FROM numbers(4)) AS t
GROUP BY t.c WITH ROLLUP
WINDOW w AS (PARTITION BY c)
ORDER BY t.c NULLS LAST, cnt, m
SETTINGS group_by_use_nulls = 0;
SELECT t.c, count() OVER (w ORDER BY t.c, count() ROWS UNBOUNDED PRECEDING) AS cnt, max(* REPLACE (intDiv(c, 2) AS c)) AS m
FROM (SELECT number AS c FROM numbers(4)) AS t
GROUP BY t.c WITH ROLLUP
WINDOW w AS (PARTITION BY c)
ORDER BY t.c NULLS LAST, cnt, m
SETTINGS group_by_use_nulls = 1;

SELECT '-- window functions before and after the matcher';
SELECT t.c, count() OVER w AS before, max(* REPLACE (intDiv(c, 2) AS c)) AS m, count() OVER w AS after
FROM (SELECT number AS c FROM numbers(4)) AS t
GROUP BY t.c WITH ROLLUP
WINDOW w AS (PARTITION BY c)
ORDER BY t.c NULLS LAST, before, m, after
SETTINGS group_by_use_nulls = 0;
SELECT t.c, count() OVER w AS before, max(* REPLACE (intDiv(c, 2) AS c)) AS m, count() OVER w AS after
FROM (SELECT number AS c FROM numbers(4)) AS t
GROUP BY t.c WITH ROLLUP
WINDOW w AS (PARTITION BY c)
ORDER BY t.c NULLS LAST, before, m, after
SETTINGS group_by_use_nulls = 1;
