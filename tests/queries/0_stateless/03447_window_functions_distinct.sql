SELECT 'Single Numeric Data:';
SELECT number, sum(DISTINCT number) OVER () FROM numbers(0, 5) ORDER BY number;
SELECT number, sum(DISTINCT number) OVER (ORDER BY number) FROM numbers(0, 5) ORDER BY number;

WITH intHash64(number) % 10 AS x
SELECT
    x,
    sumDistinct(x) OVER (ORDER BY number ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS a,
    sumDistinct(x) OVER (ORDER BY number ASC ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS b
FROM numbers(10);

SELECT 'Single Generic Plain Column:';
SELECT number, s, min(DISTINCT s) OVER(PARTITION BY number % 2) AS min_varlen_string
FROM
(
    SELECT number, toString(number % 5) AS s
    FROM numbers(11)
) ORDER BY number;

SELECT 'Single Generic Non-Plain Column:';
SELECT arr, min(DISTINCT arr) OVER (ORDER BY arr)
FROM
(
    SELECT [toString(number), toString(number+1)] AS arr
    FROM numbers(10)
) ORDER BY arr;

SELECT 'Multiple Generic Columns:';
SELECT s, ts, argMax(DISTINCT ts, s) OVER(PARTITION BY ts % 2) AS value_with_max_ts
FROM
(
    SELECT number AS ts, toString(number % 5) AS s
    FROM numbers(11)
) ORDER BY ts;

SELECT 'Aggregate State Checks:';
WITH
    arrayReduce('sumDistinctState', [1 + number, 2, 3]) AS a,
    arrayReduce('sumDistinctState', [2 + number, 3, 4]) AS b
SELECT
    finalizeAggregation(a),
    finalizeAggregation(b),
    finalizeAggregation(a + b),
    finalizeAggregation(a * 2),
    finalizeAggregation(b * 2)
FROM numbers(10);

WITH
    arrayReduce('sumDistinctState', [1, 2, 3]) AS a,
    arrayReduce('sumDistinctState', [2, 3, 4]) AS b,
    arrayReduce('sumDistinctState', [3, 4, 5]) AS c
SELECT
    finalizeAggregation(a),
    finalizeAggregation(b),
    finalizeAggregation(c),
    finalizeAggregation(a + b),
    finalizeAggregation(a + c),
    finalizeAggregation(b + c),
    finalizeAggregation((a + b) + c),
    finalizeAggregation((b + c) + a),
    finalizeAggregation((c + a) + b);
