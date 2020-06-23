set optimize_arithmetic_operations_in_aggregate_functions = 1;

SELECT sum(number * -3) + min(2 * number * -3) - max(-1 * -2 * number * -3) FROM numbers(100);
SELECT max(log(2) * number) FROM numbers(100);
SELECT round(max(log(2) * 3 * sin(0.3) * number * 4)) FROM numbers(100);

set optimize_arithmetic_operations_in_aggregate_functions = 0;

SELECT sum(number * -3) + min(2 * number * -3) - max(-1 * -2 * number * -3) FROM numbers(100);
SELECT max(log(2) * number) FROM numbers(100);
SELECT round(max(log(2) * 3 * sin(0.3) * number * 4)) FROM numbers(100);
