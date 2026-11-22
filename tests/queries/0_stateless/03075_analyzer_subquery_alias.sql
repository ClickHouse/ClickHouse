-- https://github.com/ClickHouse/ClickHouse/issues/28777
SET enable_analyzer=1;
SELECT
    sum(q0.a2) AS a1,
    sum(q0.a1) AS a9
FROM
(
    SELECT
        1 AS a1,
        2 AS a2
) AS q0;
