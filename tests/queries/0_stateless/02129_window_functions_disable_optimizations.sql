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
FROM numbers(10)
