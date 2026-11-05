SET enable_analyzer=1;
WITH x -> toString(x) AS lambda_1
SELECT
    3,
    arrayMap(lambda_1 AS lambda_2, [1, 2, 3]),
    arrayMap(lambda_2, materialize(['1', '2', '3']))
WHERE toNullable(9)
GROUP BY lambda_2; -- { serverError UNKNOWN_IDENTIFIER }
