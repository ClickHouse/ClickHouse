-- Tags: no-old-analyzer

SET join_algorithm = 'ie_join';
SET query_plan_join_swap_table = 0;

SELECT l.a
FROM
(
    SELECT number * 10 AS a, number + 1 AS b
    FROM numbers(2)
) AS l
LEFT ANTI JOIN
(
    SELECT number + 5 AS a, number + 3 AS b
    FROM numbers(1)
) AS r
ON l.a < r.a AND l.b < r.b
ORDER BY l.a;
