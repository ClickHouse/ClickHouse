SET optimize_arithmetic_operations_in_aggregate_functions = 1;

SELECT sum(number / 2) FROM numbers(10);
EXPLAIN SYNTAX SELECT sum(number / 2) FROM numbers(10);


SELECT Sum(number / 2) FROM numbers(10);
EXPLAIN SYNTAX SELECT Sum(number / 2) FROM numbers(10);
