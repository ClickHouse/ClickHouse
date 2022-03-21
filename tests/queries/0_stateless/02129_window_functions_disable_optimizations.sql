SET optimize_rewrite_sum_if_to_count_if = 1;

SELECT if(number % 10 = 0, 1, 0) AS dummy,
sum(dummy) OVER w
FROM numbers(10)
WINDOW w AS (ORDER BY number ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW);

SET optimize_arithmetic_operations_in_aggregate_functions=1;
SELECT
    *,
    if((number % 2) = 0, 0.5, 1) AS a,
    30 AS b,
    sum(a * b) OVER (ORDER BY number ASC) AS s
FROM numbers(10);

SET optimize_aggregators_of_group_by_keys=1;

SELECT
    *,
    if(number = 1, 1, 0) as a,
    max(a) OVER (ORDER BY number ASC) AS s
FROM numbers(10);

SET optimize_group_by_function_keys = 1;
SELECT round(sum(log(2) * number), 6) AS k FROM numbers(10000)
GROUP BY (number % 2) * (number % 3), number % 3, number % 2
HAVING sum(log(2) * number) > 346.57353 ORDER BY k;
