-- A matcher carried by a projection expression has to be expanded before the other clauses of the
-- query under `group_by_use_nulls`, so that its `REPLACE` mappings reach GROUP BY, HAVING and ORDER BY.
-- Two carriers besides an ordinary function argument: a qualified matcher argument of `count`, which
-- is not dropped (only an unqualified one is), and a window definition written in place.
-- Every query below is run at `group_by_use_nulls = 0` and at `group_by_use_nulls = 1`, and the two
-- results have to be the same.

SET enable_analyzer = 1;

SELECT '-- A qualified matcher stays an argument of count and its REPLACE reaches HAVING';
SELECT count(t.COLUMNS('^c$') REPLACE (100 - c AS c)) FROM (SELECT number AS c FROM numbers(3)) AS t
GROUP BY c WITH ROLLUP HAVING c > 98 ORDER BY c NULLS LAST SETTINGS group_by_use_nulls = 0;
SELECT count(t.COLUMNS('^c$') REPLACE (100 - c AS c)) FROM (SELECT number AS c FROM numbers(3)) AS t
GROUP BY c WITH ROLLUP HAVING c > 98 ORDER BY c NULLS LAST SETTINGS group_by_use_nulls = 1;

SELECT '-- The same for a qualified asterisk of a single column table expression';
SELECT count(t.* REPLACE (100 - c AS c)) FROM (SELECT number AS c FROM numbers(3)) AS t
GROUP BY c WITH ROLLUP HAVING c > 98 ORDER BY c NULLS LAST SETTINGS group_by_use_nulls = 0;
SELECT count(t.* REPLACE (100 - c AS c)) FROM (SELECT number AS c FROM numbers(3)) AS t
GROUP BY c WITH ROLLUP HAVING c > 98 ORDER BY c NULLS LAST SETTINGS group_by_use_nulls = 1;

SELECT '-- An unqualified matcher is still dropped by count, so its REPLACE stays unregistered';
SELECT count(* REPLACE (100 - c AS c)) FROM (SELECT number AS c FROM numbers(3))
GROUP BY GROUPING SETS ((c)) HAVING c < 2 ORDER BY c SETTINGS group_by_use_nulls = 0;
SELECT count(* REPLACE (100 - c AS c)) FROM (SELECT number AS c FROM numbers(3))
GROUP BY GROUPING SETS ((c)) HAVING c < 2 ORDER BY c SETTINGS group_by_use_nulls = 1;

SELECT '-- A matcher in the PARTITION BY of a window definition written in place';
SELECT count() OVER (PARTITION BY max(* REPLACE (100 - c AS c))) AS w
FROM (SELECT number AS c FROM numbers(3))
GROUP BY GROUPING SETS ((c)) HAVING c > 98 ORDER BY w SETTINGS group_by_use_nulls = 0;
SELECT count() OVER (PARTITION BY max(* REPLACE (100 - c AS c))) AS w
FROM (SELECT number AS c FROM numbers(3))
GROUP BY GROUPING SETS ((c)) HAVING c > 98 ORDER BY w SETTINGS group_by_use_nulls = 1;

SELECT '-- A matcher in the ORDER BY of a window definition written in place';
SELECT count() OVER (ORDER BY max(* REPLACE (100 - c AS c))) AS w
FROM (SELECT number AS c FROM numbers(3))
GROUP BY GROUPING SETS ((c)) HAVING c > 98 ORDER BY w SETTINGS group_by_use_nulls = 0;
SELECT count() OVER (ORDER BY max(* REPLACE (100 - c AS c))) AS w
FROM (SELECT number AS c FROM numbers(3))
GROUP BY GROUPING SETS ((c)) HAVING c > 98 ORDER BY w SETTINGS group_by_use_nulls = 1;

SELECT '-- A matcher in a window definition written in place, with the ROLLUP total';
SELECT count() OVER (PARTITION BY max(COLUMNS('^c$') REPLACE (c % 2 AS c))) AS w
FROM (SELECT number AS c FROM numbers(4))
GROUP BY c WITH ROLLUP ORDER BY w SETTINGS group_by_use_nulls = 0;
SELECT count() OVER (PARTITION BY max(COLUMNS('^c$') REPLACE (c % 2 AS c))) AS w
FROM (SELECT number AS c FROM numbers(4))
GROUP BY c WITH ROLLUP ORDER BY w SETTINGS group_by_use_nulls = 1;

SELECT '-- A matcher in the definition of a named window from the WINDOW clause';
SELECT count() OVER w AS x
FROM (SELECT number AS c FROM numbers(3))
GROUP BY GROUPING SETS ((c)) HAVING c > 98
WINDOW w AS (PARTITION BY max(* REPLACE (100 - c AS c)))
ORDER BY x SETTINGS group_by_use_nulls = 0;
SELECT count() OVER w AS x
FROM (SELECT number AS c FROM numbers(3))
GROUP BY GROUPING SETS ((c)) HAVING c > 98
WINDOW w AS (PARTITION BY max(* REPLACE (100 - c AS c)))
ORDER BY x SETTINGS group_by_use_nulls = 1;

SELECT '-- The same for a window derived from a named one';
SELECT count() OVER (w ORDER BY max(c)) AS x
FROM (SELECT number AS c FROM numbers(3))
GROUP BY GROUPING SETS ((c)) HAVING c > 98
WINDOW w AS (PARTITION BY max(* REPLACE (100 - c AS c)))
ORDER BY x SETTINGS group_by_use_nulls = 0;
SELECT count() OVER (w ORDER BY max(c)) AS x
FROM (SELECT number AS c FROM numbers(3))
GROUP BY GROUPING SETS ((c)) HAVING c > 98
WINDOW w AS (PARTITION BY max(* REPLACE (100 - c AS c)))
ORDER BY x SETTINGS group_by_use_nulls = 1;
