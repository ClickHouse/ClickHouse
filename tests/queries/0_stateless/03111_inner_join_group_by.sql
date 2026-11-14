-- https://github.com/ClickHouse/ClickHouse/issues/50705

set enable_analyzer=1;

SELECT
    count(s0.number),
    s1.half
FROM system.numbers AS s0
INNER JOIN
(
    SELECT
        number,
        number / 2 AS half
    FROM system.numbers
    LIMIT 10
) AS s1 ON s0.number = s1.number
GROUP BY s0.number > 5
LIMIT 10 -- {serverError NOT_AN_AGGREGATE}
