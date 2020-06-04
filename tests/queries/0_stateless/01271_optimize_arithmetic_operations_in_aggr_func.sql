set optimize_ast_arithmetic = 1;

SELECT sum(number * -3) + min(2 * number * -3) - max(-1 * -2 * number * -3) FROM numbers(10000000);
SELECT max(log(2) * number) FROM numbers(10000000);
SELECT round(max(log(2) * 3 * sin(0.3) * number * 4)) FROM numbers(10000000);

set optimize_ast_arithmetic = 0;

SELECT sum(number * -3) + min(2 * number * -3) - max(-1 * -2 * number * -3) FROM numbers(10000000);
SELECT max(log(2) * number) FROM numbers(10000000);
SELECT round(max(log(2) * 3 * sin(0.3) * number * 4)) FROM numbers(10000000);
