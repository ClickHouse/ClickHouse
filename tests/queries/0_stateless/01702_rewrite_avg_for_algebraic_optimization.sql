SET optimize_arithmetic_operations_in_aggregate_functions = 1;

SELECT 'SELECT avg(number + 2) FROM numbers(10)';
SELECT 'value: ', avg(number + 2) FROM numbers(10);
SELECT 'EXPLAIN syntax:';
EXPLAIN SYNTAX SELECT avg(number + 2) FROM numbers(10);

SELECT '';
SELECT 'SELECT avg(number - 2) FROM numbers(10)';
SELECT 'value: ', avg(number - 2) FROM numbers(10);
SELECT 'EXPLAIN syntax:';
EXPLAIN SYNTAX SELECT avg(number - 2) FROM numbers(10);

SELECT '';
SELECT 'SELECT avg(number * 2) FROM numbers(10)';
SELECT 'value: ', avg(number * 2) FROM numbers(10);
SELECT 'EXPLAIN syntax:';
EXPLAIN SYNTAX SELECT avg(number * 2) FROM numbers(10);

SELECT '';
SELECT 'SELECT avg(number / 2) FROM numbers(10)';
SELECT 'value: ', avg(number / 2) FROM numbers(10);
SELECT 'EXPLAIN syntax:';
EXPLAIN SYNTAX SELECT avg(number / 2) FROM numbers(10);
