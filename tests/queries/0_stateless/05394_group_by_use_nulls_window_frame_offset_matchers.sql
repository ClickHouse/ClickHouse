-- The matchers in the frame offsets of a window are expanded before the other clauses of the query
-- with `group_by_use_nulls` as well, so their `REPLACE` transformers rewrite `HAVING` the same way.

SELECT count() OVER (ORDER BY t.c ROWS toUInt64(* REPLACE (100 AS c)) PRECEDING) AS cnt
FROM (SELECT number AS c FROM numbers(3)) AS t
GROUP BY GROUPING SETS ((t.c))
HAVING c > 98
ORDER BY t.c
SETTINGS group_by_use_nulls = 0;

SELECT '--';

SELECT count() OVER (ORDER BY t.c ROWS toUInt64(* REPLACE (100 AS c)) PRECEDING) AS cnt
FROM (SELECT number AS c FROM numbers(3)) AS t
GROUP BY GROUPING SETS ((t.c))
HAVING c > 98
ORDER BY t.c
SETTINGS group_by_use_nulls = 1;

SELECT '--';

SELECT count() OVER (ORDER BY t.c ROWS BETWEEN CURRENT ROW AND toUInt64(* REPLACE (100 AS c)) FOLLOWING) AS cnt
FROM (SELECT number AS c FROM numbers(3)) AS t
GROUP BY GROUPING SETS ((t.c))
HAVING c > 98
ORDER BY t.c
SETTINGS group_by_use_nulls = 1;

SELECT '--';

SELECT count() OVER w AS cnt
FROM (SELECT number AS c FROM numbers(3)) AS t
GROUP BY GROUPING SETS ((t.c))
HAVING c > 98
WINDOW w AS (ORDER BY t.c ROWS toUInt64(* REPLACE (100 AS c)) PRECEDING)
ORDER BY t.c
SETTINGS group_by_use_nulls = 0;

SELECT '--';

SELECT count() OVER w AS cnt
FROM (SELECT number AS c FROM numbers(3)) AS t
GROUP BY GROUPING SETS ((t.c))
HAVING c > 98
WINDOW w AS (ORDER BY t.c ROWS toUInt64(* REPLACE (100 AS c)) PRECEDING)
ORDER BY t.c
SETTINGS group_by_use_nulls = 1;
