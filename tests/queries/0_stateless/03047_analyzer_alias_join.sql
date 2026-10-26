SET enable_analyzer=1;
SELECT
    1 AS value,
    *
FROM
(
    SELECT 1 AS key
) AS l
LEFT JOIN
(
    SELECT
        2 AS key,
        1 AS value
) AS r USING (key)
SETTINGS join_use_nulls = 1;

SELECT
    1 AS value,
    *
FROM
(
    SELECT 2 AS key
) AS l
LEFT JOIN
(
    SELECT
        2 AS key,
        1 AS value
) AS r USING (key)
SETTINGS join_use_nulls = 1
