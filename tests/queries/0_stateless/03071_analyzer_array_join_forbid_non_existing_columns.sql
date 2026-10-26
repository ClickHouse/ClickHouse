-- https://github.com/ClickHouse/ClickHouse/issues/9233
SET enable_analyzer=1;
SELECT *
FROM
(
    SELECT
        [1, 2, 3] AS x,
        [4, 5, 6] AS y
)
ARRAY JOIN
    x,
    Y; -- { serverError UNKNOWN_IDENTIFIER }
