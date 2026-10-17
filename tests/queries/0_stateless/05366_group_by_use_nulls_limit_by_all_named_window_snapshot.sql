-- `LIMIT BY ALL` referring to a window function over a named window uses the definition of the window
-- as of the projection item, not the one rewritten by a following `REPLACE`.

SET enable_analyzer = 1;
SET enable_positional_arguments = 1;

-- The values of `c` start from 1, so that the `ROLLUP` row does not share a partition with a regular row
-- when `group_by_use_nulls = 0`, and the window functions do not depend on the order of the rows within a partition.

SELECT row_number() OVER w AS rn, max(* REPLACE (intDiv(c, 2) AS c)) AS m
FROM (SELECT number + 1 AS c FROM numbers(4)) AS t
GROUP BY t.c WITH ROLLUP
WINDOW w AS (PARTITION BY c ORDER BY c)
ORDER BY 1, 2
LIMIT 1 BY ALL
SETTINGS group_by_use_nulls = 0;

SELECT row_number() OVER w AS rn, max(* REPLACE (intDiv(c, 2) AS c)) AS m
FROM (SELECT number + 1 AS c FROM numbers(4)) AS t
GROUP BY t.c WITH ROLLUP
WINDOW w AS (PARTITION BY c ORDER BY c)
ORDER BY 1, 2
LIMIT 1 BY ALL
SETTINGS group_by_use_nulls = 1;
