SET enable_analyzer=1;
SET optimize_arithmetic_operations_in_aggregate_functions=1;
SET allow_reorder_prewhere_conditions = 0;

EXPLAIN actions = 1, compact = 1
SELECT number % 10 AS key, sum(number * 2) AS total
FROM numbers(1000)
WHERE number > 100
GROUP BY key
ORDER BY total DESC
LIMIT 5;

EXPLAIN actions = 1, pretty = 1
SELECT number % 10 AS key, sum(number * 2) AS total
FROM numbers(1000)
WHERE number > 100
GROUP BY key
ORDER BY total DESC
LIMIT 5;

EXPLAIN actions = 1, pretty = 1, compact = 1
SELECT number % 10 AS key, sum(number * 2) AS total
FROM numbers(1000)
WHERE number > 100
GROUP BY key
ORDER BY total DESC
LIMIT 5;