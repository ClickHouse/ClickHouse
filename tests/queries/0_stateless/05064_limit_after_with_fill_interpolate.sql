-- `INTERPOLATE` writes its values into the column named after the interpolated alias, so a range
-- boundary that refers to that alias must see the interpolated values on the filled rows, exactly as the
-- query result does. The filled rows here are `(1, 1)`, `(3, 11)` and `(5, 21)`.
SET max_block_size = 1;
SET max_threads = 1;

SELECT number * 2 AS n, number * 10 AS v
FROM numbers(3)
ORDER BY n WITH FILL FROM 0 TO 5 INTERPOLATE (v AS v + 1)
LIMIT AFTER v >= 11
SETTINGS enable_analyzer = 1;

SELECT number * 2 AS n, number * 10 AS v
FROM numbers(3)
ORDER BY n WITH FILL FROM 0 TO 5 INTERPOLATE (v AS v + 1)
LIMIT AFTER v >= 11
SETTINGS enable_analyzer = 0;

SELECT number * 2 AS n, number * 10 AS v
FROM numbers(3)
ORDER BY n WITH FILL FROM 0 TO 5 INTERPOLATE (v AS v + 1)
LIMIT UNTIL v >= 11
SETTINGS enable_analyzer = 1;

SELECT number * 2 AS n, number * 10 AS v
FROM numbers(3)
ORDER BY n WITH FILL FROM 0 TO 5 INTERPOLATE (v AS v + 1)
LIMIT UNTIL v >= 11
SETTINGS enable_analyzer = 0;

-- Every `ALL` match here is a filled row.
SELECT groupArray((n, v))
FROM
(
    SELECT number * 2 AS n, number * 10 AS v
    FROM numbers(3)
    ORDER BY n WITH FILL FROM 0 TO 6 STEP 1 INTERPOLATE (v AS v + 1)
    LIMIT 1 AFTER v IN (1, 11, 21) ALL
)
SETTINGS enable_analyzer = 1;

SELECT groupArray((n, v))
FROM
(
    SELECT number * 2 AS n, number * 10 AS v
    FROM numbers(3)
    ORDER BY n WITH FILL FROM 0 TO 6 STEP 1 INTERPOLATE (v AS v + 1)
    LIMIT 1 AFTER v IN (1, 11, 21) ALL
)
SETTINGS enable_analyzer = 0;

-- A boundary over an interpolated alias together with the fill column.
SELECT number * 2 AS n, number * 10 AS v
FROM numbers(3)
ORDER BY n WITH FILL FROM 0 TO 6 INTERPOLATE (v AS v + 1)
LIMIT AFTER v = 11 UNTIL n = 5
SETTINGS enable_analyzer = 1;

SELECT number * 2 AS n, number * 10 AS v
FROM numbers(3)
ORDER BY n WITH FILL FROM 0 TO 6 INTERPOLATE (v AS v + 1)
LIMIT AFTER v = 11 UNTIL n = 5
SETTINGS enable_analyzer = 0;
