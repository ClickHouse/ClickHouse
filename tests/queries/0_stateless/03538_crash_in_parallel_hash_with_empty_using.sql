set  enable_parallel_replicas = 0;

SELECT
    n,
    myfield
FROM
(
    SELECT toString(number) AS n
    FROM system.numbers
    LIMIT 1000
) AS a
ANY LEFT JOIN
(
    SELECT 1 AS myfield
) AS b USING ()
FORMAT Null
SETTINGS allow_experimental_analyzer = false;

SELECT
    n,
    myfield
FROM
(
    SELECT toString(number) AS n
    FROM system.numbers
    LIMIT 1000
) AS a
ANY LEFT JOIN
(
    SELECT 1 AS myfield
) AS b USING ()
FORMAT Null
SETTINGS allow_experimental_analyzer = true; -- { serverError INVALID_JOIN_ON_EXPRESSION }
