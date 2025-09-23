SET enable_analyzer = 1;

WITH table AS
    (
        SELECT 1 AS key
    )
SELECT *
FROM table AS T1
LEFT JOIN
(
    SELECT *
    FROM table
    WHERE false
) AS T2 ON 1;

WITH table AS
    (
        SELECT 1 AS key
    )
SELECT *
FROM table AS T1
LEFT JOIN
(
    SELECT *
    FROM table
    WHERE false
) AS T2 ON 0;
