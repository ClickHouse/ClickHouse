-- `LIMIT BY ALL` referring to a window function over a named window uses the definition of the window
-- as of the projection item, not the one rewritten by a following `REPLACE`.

SET enable_analyzer = 1;

SELECT row_number() OVER w AS rn, max(* REPLACE (intDiv(c, 2) AS c)) AS m
FROM (SELECT number AS c FROM numbers(4)) AS t
GROUP BY t.c WITH ROLLUP
WINDOW w AS (PARTITION BY c ORDER BY c)
ORDER BY rn, m
LIMIT 1 BY ALL
SETTINGS group_by_use_nulls = 0;

SELECT row_number() OVER w AS rn, max(* REPLACE (intDiv(c, 2) AS c)) AS m
FROM (SELECT number AS c FROM numbers(4)) AS t
GROUP BY t.c WITH ROLLUP
WINDOW w AS (PARTITION BY c ORDER BY c)
ORDER BY rn, m
LIMIT 1 BY ALL
SETTINGS group_by_use_nulls = 1;
