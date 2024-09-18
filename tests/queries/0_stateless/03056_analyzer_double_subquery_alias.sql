-- https://github.com/ClickHouse/ClickHouse/issues/22627
SET allow_experimental_analyzer=1;
WITH
    x AS
    (
        SELECT 1 AS a
    ),
    xx AS
    (
        SELECT *
        FROM x
        , x AS x1
        , x AS x2
    )
SELECT *
FROM xx
WHERE a = 1;
