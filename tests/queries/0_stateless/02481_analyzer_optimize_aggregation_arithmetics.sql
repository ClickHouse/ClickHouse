SET enable_analyzer = 1;
SET optimize_arithmetic_operations_in_aggregate_functions = 1;

-- { echoOn }

EXPLAIN QUERY TREE SELECT avg(log(2) * number) FROM numbers(10);

EXPLAIN QUERY TREE SELECT avg(number * log(2)) FROM numbers(10);

SELECT round(avg(log(2) * number), 6) AS k FROM numbers(10000000) GROUP BY number % 3, number % 2;

SELECT round(avg(number * log(2)), 6) AS k FROM numbers(10000000) GROUP BY number % 3, number % 2;

-- { echoOff }
