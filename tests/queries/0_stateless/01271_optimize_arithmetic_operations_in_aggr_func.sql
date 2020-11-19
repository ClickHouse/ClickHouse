SET enable_debug_queries = 1;
SET optimize_arithmetic_operations_in_aggregate_functions = 1;

ANALYZE SELECT sum(n + 1), sum(1 + n), sum(n - 1), sum(1 - n) FROM (SELECT number n FROM numbers(10));
ANALYZE SELECT sum(n * 2), sum(2 * n), sum(n / 2), sum(1 / n) FROM (SELECT number n FROM numbers(10));
ANALYZE SELECT min(n + 1), min(1 + n), min(n - 1), min(1 - n) FROM (SELECT number n FROM numbers(10));
ANALYZE SELECT min(n * 2), min(2 * n), min(n / 2), min(1 / n) FROM (SELECT number n FROM numbers(10));
ANALYZE SELECT max(n + 1), max(1 + n), max(n - 1), max(1 - n) FROM (SELECT number n FROM numbers(10));
ANALYZE SELECT max(n * 2), max(2 * n), max(n / 2), max(1 / n) FROM (SELECT number n FROM numbers(10));

ANALYZE SELECT sum(n + -1), sum(-1 + n), sum(n - -1), sum(-1 - n) FROM (SELECT number n FROM numbers(10));
ANALYZE SELECT sum(n * -2), sum(-2 * n), sum(n / -2), sum(-1 / n) FROM (SELECT number n FROM numbers(10));
ANALYZE SELECT min(n + -1), min(-1 + n), min(n - -1), min(-1 - n) FROM (SELECT number n FROM numbers(10));
ANALYZE SELECT min(n * -2), min(-2 * n), min(n / -2), min(-1 / n) FROM (SELECT number n FROM numbers(10));
ANALYZE SELECT max(n + -1), max(-1 + n), max(n - -1), max(-1 - n) FROM (SELECT number n FROM numbers(10));
ANALYZE SELECT max(n * -2), max(-2 * n), max(n / -2), max(-1 / n) FROM (SELECT number n FROM numbers(10));

ANALYZE SELECT sum(abs(2) + 1), sum(abs(2) + n), sum(n - abs(2)), sum(1 - abs(2)) FROM (SELECT number n FROM numbers(10));
ANALYZE SELECT sum(abs(2) * 2), sum(abs(2) * n), sum(n / abs(2)), sum(1 / abs(2)) FROM (SELECT number n FROM numbers(10));
ANALYZE SELECT min(abs(2) + 1), min(abs(2) + n), min(n - abs(2)), min(1 - abs(2)) FROM (SELECT number n FROM numbers(10));
ANALYZE SELECT min(abs(2) * 2), min(abs(2) * n), min(n / abs(2)), min(1 / abs(2)) FROM (SELECT number n FROM numbers(10));
ANALYZE SELECT max(abs(2) + 1), max(abs(2) + n), max(n - abs(2)), max(1 - abs(2)) FROM (SELECT number n FROM numbers(10));
ANALYZE SELECT max(abs(2) * 2), max(abs(2) * n), max(n / abs(2)), max(1 / abs(2)) FROM (SELECT number n FROM numbers(10));

ANALYZE SELECT sum(abs(n) + 1), sum(abs(n) + n), sum(n - abs(n)), sum(1 - abs(n)) FROM (SELECT number n FROM numbers(10));
ANALYZE SELECT sum(abs(n) * 2), sum(abs(n) * n), sum(n / abs(n)), sum(1 / abs(n)) FROM (SELECT number n FROM numbers(10));
ANALYZE SELECT min(abs(n) + 1), min(abs(n) + n), min(n - abs(n)), min(1 - abs(n)) FROM (SELECT number n FROM numbers(10));
ANALYZE SELECT min(abs(n) * 2), min(abs(n) * n), min(n / abs(n)), min(1 / abs(n)) FROM (SELECT number n FROM numbers(10));
ANALYZE SELECT max(abs(n) + 1), max(abs(n) + n), max(n - abs(n)), max(1 - abs(n)) FROM (SELECT number n FROM numbers(10));
ANALYZE SELECT max(abs(n) * 2), max(abs(n) * n), max(n / abs(n)), max(1 / abs(n)) FROM (SELECT number n FROM numbers(10));

ANALYZE SELECT sum(n*n + 1), sum(1 + n*n), sum(n*n - 1), sum(1 - n*n) FROM (SELECT number n FROM numbers(10));
ANALYZE SELECT sum(n*n * 2), sum(2 * n*n), sum(n*n / 2), sum(1 / n*n) FROM (SELECT number n FROM numbers(10));
ANALYZE SELECT min(n*n + 1), min(1 + n*n), min(n*n - 1), min(1 - n*n) FROM (SELECT number n FROM numbers(10));
ANALYZE SELECT min(n*n * 2), min(2 * n*n), min(n*n / 2), min(1 / n*n) FROM (SELECT number n FROM numbers(10));
ANALYZE SELECT max(n*n + 1), max(1 + n*n), max(n*n - 1), max(1 - n*n) FROM (SELECT number n FROM numbers(10));
ANALYZE SELECT max(n*n * 2), max(2 * n*n), max(n*n / 2), max(1 / n*n) FROM (SELECT number n FROM numbers(10));

ANALYZE SELECT sum(1 + n + 1), sum(1 + 1 + n), sum(1 + n - 1), sum(1 + 1 - n) FROM (SELECT number n FROM numbers(10));
ANALYZE SELECT sum(1 + n * 2), sum(1 + 2 * n), sum(1 + n / 2), sum(1 + 1 / n) FROM (SELECT number n FROM numbers(10));
ANALYZE SELECT min(1 + n + 1), min(1 + 1 + n), min(1 + n - 1), min(1 + 1 - n) FROM (SELECT number n FROM numbers(10));
ANALYZE SELECT min(1 + n * 2), min(1 + 2 * n), min(1 + n / 2), min(1 + 1 / n) FROM (SELECT number n FROM numbers(10));
ANALYZE SELECT max(1 + n + 1), max(1 + 1 + n), max(1 + n - 1), max(1 + 1 - n) FROM (SELECT number n FROM numbers(10));
ANALYZE SELECT max(1 + n * 2), max(1 + 2 * n), max(1 + n / 2), max(1 + 1 / n) FROM (SELECT number n FROM numbers(10));

ANALYZE SELECT sum(n + -1 + -1), sum(-1 + n + -1), sum(n - -1 + -1), sum(-1 - n + -1) FROM (SELECT number n FROM numbers(10));
ANALYZE SELECT sum(n * -2 * -1), sum(-2 * n * -1), sum(n / -2 / -1), sum(-1 / n / -1) FROM (SELECT number n FROM numbers(10));
ANALYZE SELECT min(n + -1 + -1), min(-1 + n + -1), min(n - -1 + -1), min(-1 - n + -1) FROM (SELECT number n FROM numbers(10));
ANALYZE SELECT min(n * -2 * -1), min(-2 * n * -1), min(n / -2 / -1), min(-1 / n / -1) FROM (SELECT number n FROM numbers(10));
ANALYZE SELECT max(n + -1 + -1), max(-1 + n + -1), max(n - -1 + -1), max(-1 - n + -1) FROM (SELECT number n FROM numbers(10));
ANALYZE SELECT max(n * -2 * -1), max(-2 * n * -1), max(n / -2 / -1), max(-1 / n / -1) FROM (SELECT number n FROM numbers(10));

ANALYZE SELECT sum(n + 1) + sum(1 + n) + sum(n - 1) + sum(1 - n) FROM (SELECT number n FROM numbers(10));
ANALYZE SELECT sum(n * 2) + sum(2 * n) + sum(n / 2) + sum(1 / n) FROM (SELECT number n FROM numbers(10));
ANALYZE SELECT min(n + 1) + min(1 + n) + min(n - 1) + min(1 - n) FROM (SELECT number n FROM numbers(10));
ANALYZE SELECT min(n * 2) + min(2 * n) + min(n / 2) + min(1 / n) FROM (SELECT number n FROM numbers(10));
ANALYZE SELECT max(n + 1) + max(1 + n) + max(n - 1) + max(1 - n) FROM (SELECT number n FROM numbers(10));
ANALYZE SELECT max(n * 2) + max(2 * n) + max(n / 2) + max(1 / n) FROM (SELECT number n FROM numbers(10));


SELECT sum(n + 1), sum(1 + n), sum(n - 1), sum(1 - n) FROM (SELECT number n FROM numbers(10));
SELECT sum(n * 2), sum(2 * n), sum(n / 2), sum(1 / n) FROM (SELECT number n FROM numbers(10));
SELECT min(n + 1), min(1 + n), min(n - 1), min(1 - n) FROM (SELECT number n FROM numbers(10));
SELECT min(n * 2), min(2 * n), min(n / 2), min(1 / n) FROM (SELECT number n FROM numbers(10));
SELECT max(n + 1), max(1 + n), max(n - 1), max(1 - n) FROM (SELECT number n FROM numbers(10));
SELECT max(n * 2), max(2 * n), max(n / 2), max(1 / n) FROM (SELECT number n FROM numbers(10));

SELECT sum(n + -1), sum(-1 + n), sum(n - -1), sum(-1 - n) FROM (SELECT number n FROM numbers(10));
SELECT sum(n * -2), sum(-2 * n), sum(n / -2), sum(-1 / n) FROM (SELECT number n FROM numbers(10));
SELECT min(n + -1), min(-1 + n), min(n - -1), min(-1 - n) FROM (SELECT number n FROM numbers(10));
SELECT min(n * -2), min(-2 * n), min(n / -2), min(-1 / n) FROM (SELECT number n FROM numbers(10));
SELECT max(n + -1), max(-1 + n), max(n - -1), max(-1 - n) FROM (SELECT number n FROM numbers(10));
SELECT max(n * -2), max(-2 * n), max(n / -2), max(-1 / n) FROM (SELECT number n FROM numbers(10));

SELECT sum(abs(2) + 1), sum(abs(2) + n), sum(n - abs(2)), sum(1 - abs(2)) FROM (SELECT number n FROM numbers(10));
SELECT sum(abs(2) * 2), sum(abs(2) * n), sum(n / abs(2)), sum(1 / abs(2)) FROM (SELECT number n FROM numbers(10));
SELECT min(abs(2) + 1), min(abs(2) + n), min(n - abs(2)), min(1 - abs(2)) FROM (SELECT number n FROM numbers(10));
SELECT min(abs(2) * 2), min(abs(2) * n), min(n / abs(2)), min(1 / abs(2)) FROM (SELECT number n FROM numbers(10));
SELECT max(abs(2) + 1), max(abs(2) + n), max(n - abs(2)), max(1 - abs(2)) FROM (SELECT number n FROM numbers(10));
SELECT max(abs(2) * 2), max(abs(2) * n), max(n / abs(2)), max(1 / abs(2)) FROM (SELECT number n FROM numbers(10));

SELECT sum(abs(n) + 1), sum(abs(n) + n), sum(n - abs(n)), sum(1 - abs(n)) FROM (SELECT number n FROM numbers(10));
SELECT sum(abs(n) * 2), sum(abs(n) * n), sum(n / abs(n)), sum(1 / abs(n)) FROM (SELECT number n FROM numbers(10));
SELECT min(abs(n) + 1), min(abs(n) + n), min(n - abs(n)), min(1 - abs(n)) FROM (SELECT number n FROM numbers(10));
SELECT min(abs(n) * 2), min(abs(n) * n), min(n / abs(n)), min(1 / abs(n)) FROM (SELECT number n FROM numbers(10));
SELECT max(abs(n) + 1), max(abs(n) + n), max(n - abs(n)), max(1 - abs(n)) FROM (SELECT number n FROM numbers(10));
SELECT max(abs(n) * 2), max(abs(n) * n), max(n / abs(n)), max(1 / abs(n)) FROM (SELECT number n FROM numbers(10));

SELECT sum(n*n + 1), sum(1 + n*n), sum(n*n - 1), sum(1 - n*n) FROM (SELECT number n FROM numbers(10));
SELECT sum(n*n * 2), sum(2 * n*n), sum(n*n / 2), sum(1 / n*n) FROM (SELECT number n FROM numbers(10));
SELECT min(n*n + 1), min(1 + n*n), min(n*n - 1), min(1 - n*n) FROM (SELECT number n FROM numbers(10));
SELECT min(n*n * 2), min(2 * n*n), min(n*n / 2), min(1 / n*n) FROM (SELECT number n FROM numbers(10));
SELECT max(n*n + 1), max(1 + n*n), max(n*n - 1), max(1 - n*n) FROM (SELECT number n FROM numbers(10));
SELECT max(n*n * 2), max(2 * n*n), max(n*n / 2), max(1 / n*n) FROM (SELECT number n FROM numbers(10));

SELECT sum(1 + n + 1), sum(1 + 1 + n), sum(1 + n - 1), sum(1 + 1 - n) FROM (SELECT number n FROM numbers(10));
SELECT sum(1 + n * 2), sum(1 + 2 * n), sum(1 + n / 2), sum(1 + 1 / n) FROM (SELECT number n FROM numbers(10));
SELECT min(1 + n + 1), min(1 + 1 + n), min(1 + n - 1), min(1 + 1 - n) FROM (SELECT number n FROM numbers(10));
SELECT min(1 + n * 2), min(1 + 2 * n), min(1 + n / 2), min(1 + 1 / n) FROM (SELECT number n FROM numbers(10));
SELECT max(1 + n + 1), max(1 + 1 + n), max(1 + n - 1), max(1 + 1 - n) FROM (SELECT number n FROM numbers(10));
SELECT max(1 + n * 2), max(1 + 2 * n), max(1 + n / 2), max(1 + 1 / n) FROM (SELECT number n FROM numbers(10));

SELECT sum(n + -1 + -1), sum(-1 + n + -1), sum(n - -1 + -1), sum(-1 - n + -1) FROM (SELECT number n FROM numbers(10));
SELECT sum(n * -2 * -1), sum(-2 * n * -1), sum(n / -2 / -1), sum(-1 / n / -1) FROM (SELECT number n FROM numbers(10));
SELECT min(n + -1 + -1), min(-1 + n + -1), min(n - -1 + -1), min(-1 - n + -1) FROM (SELECT number n FROM numbers(10));
SELECT min(n * -2 * -1), min(-2 * n * -1), min(n / -2 / -1), min(-1 / n / -1) FROM (SELECT number n FROM numbers(10));
SELECT max(n + -1 + -1), max(-1 + n + -1), max(n - -1 + -1), max(-1 - n + -1) FROM (SELECT number n FROM numbers(10));
SELECT max(n * -2 * -1), max(-2 * n * -1), max(n / -2 / -1), max(-1 / n / -1) FROM (SELECT number n FROM numbers(10));

SELECT sum(n + 1) + sum(1 + n) + sum(n - 1) + sum(1 - n) FROM (SELECT number n FROM numbers(10));
SELECT sum(n * 2) + sum(2 * n) + sum(n / 2) + sum(1 / n) FROM (SELECT number n FROM numbers(10));
SELECT min(n + 1) + min(1 + n) + min(n - 1) + min(1 - n) FROM (SELECT number n FROM numbers(10));
SELECT min(n * 2) + min(2 * n) + min(n / 2) + min(1 / n) FROM (SELECT number n FROM numbers(10));
SELECT max(n + 1) + max(1 + n) + max(n - 1) + max(1 - n) FROM (SELECT number n FROM numbers(10));
SELECT max(n * 2) + max(2 * n) + max(n / 2) + max(1 / n) FROM (SELECT number n FROM numbers(10));


SELECT sum(number * -3) + min(2 * number * -3) - max(-1 * -2 * number * -3) FROM numbers(100);
SELECT max(log(2) * number) FROM numbers(100);
SELECT round(max(log(2) * 3 * sin(0.3) * number * 4)) FROM numbers(100);

SET optimize_arithmetic_operations_in_aggregate_functions = 0;

SELECT sum(n + 1), sum(1 + n), sum(n - 1), sum(1 - n) FROM (SELECT number n FROM numbers(10));
SELECT sum(n * 2), sum(2 * n), sum(n / 2), sum(1 / n) FROM (SELECT number n FROM numbers(10));
SELECT min(n + 1), min(1 + n), min(n - 1), min(1 - n) FROM (SELECT number n FROM numbers(10));
SELECT min(n * 2), min(2 * n), min(n / 2), min(1 / n) FROM (SELECT number n FROM numbers(10));
SELECT max(n + 1), max(1 + n), max(n - 1), max(1 - n) FROM (SELECT number n FROM numbers(10));
SELECT max(n * 2), max(2 * n), max(n / 2), max(1 / n) FROM (SELECT number n FROM numbers(10));

SELECT sum(n + -1), sum(-1 + n), sum(n - -1), sum(-1 - n) FROM (SELECT number n FROM numbers(10));
SELECT sum(n * -2), sum(-2 * n), sum(n / -2), sum(-1 / n) FROM (SELECT number n FROM numbers(10));
SELECT min(n + -1), min(-1 + n), min(n - -1), min(-1 - n) FROM (SELECT number n FROM numbers(10));
SELECT min(n * -2), min(-2 * n), min(n / -2), min(-1 / n) FROM (SELECT number n FROM numbers(10));
SELECT max(n + -1), max(-1 + n), max(n - -1), max(-1 - n) FROM (SELECT number n FROM numbers(10));
SELECT max(n * -2), max(-2 * n), max(n / -2), max(-1 / n) FROM (SELECT number n FROM numbers(10));

SELECT sum(abs(2) + 1), sum(abs(2) + n), sum(n - abs(2)), sum(1 - abs(2)) FROM (SELECT number n FROM numbers(10));
SELECT sum(abs(2) * 2), sum(abs(2) * n), sum(n / abs(2)), sum(1 / abs(2)) FROM (SELECT number n FROM numbers(10));
SELECT min(abs(2) + 1), min(abs(2) + n), min(n - abs(2)), min(1 - abs(2)) FROM (SELECT number n FROM numbers(10));
SELECT min(abs(2) * 2), min(abs(2) * n), min(n / abs(2)), min(1 / abs(2)) FROM (SELECT number n FROM numbers(10));
SELECT max(abs(2) + 1), max(abs(2) + n), max(n - abs(2)), max(1 - abs(2)) FROM (SELECT number n FROM numbers(10));
SELECT max(abs(2) * 2), max(abs(2) * n), max(n / abs(2)), max(1 / abs(2)) FROM (SELECT number n FROM numbers(10));

SELECT sum(abs(n) + 1), sum(abs(n) + n), sum(n - abs(n)), sum(1 - abs(n)) FROM (SELECT number n FROM numbers(10));
SELECT sum(abs(n) * 2), sum(abs(n) * n), sum(n / abs(n)), sum(1 / abs(n)) FROM (SELECT number n FROM numbers(10));
SELECT min(abs(n) + 1), min(abs(n) + n), min(n - abs(n)), min(1 - abs(n)) FROM (SELECT number n FROM numbers(10));
SELECT min(abs(n) * 2), min(abs(n) * n), min(n / abs(n)), min(1 / abs(n)) FROM (SELECT number n FROM numbers(10));
SELECT max(abs(n) + 1), max(abs(n) + n), max(n - abs(n)), max(1 - abs(n)) FROM (SELECT number n FROM numbers(10));
SELECT max(abs(n) * 2), max(abs(n) * n), max(n / abs(n)), max(1 / abs(n)) FROM (SELECT number n FROM numbers(10));

SELECT sum(n*n + 1), sum(1 + n*n), sum(n*n - 1), sum(1 - n*n) FROM (SELECT number n FROM numbers(10));
SELECT sum(n*n * 2), sum(2 * n*n), sum(n*n / 2), sum(1 / n*n) FROM (SELECT number n FROM numbers(10));
SELECT min(n*n + 1), min(1 + n*n), min(n*n - 1), min(1 - n*n) FROM (SELECT number n FROM numbers(10));
SELECT min(n*n * 2), min(2 * n*n), min(n*n / 2), min(1 / n*n) FROM (SELECT number n FROM numbers(10));
SELECT max(n*n + 1), max(1 + n*n), max(n*n - 1), max(1 - n*n) FROM (SELECT number n FROM numbers(10));
SELECT max(n*n * 2), max(2 * n*n), max(n*n / 2), max(1 / n*n) FROM (SELECT number n FROM numbers(10));

SELECT sum(1 + n + 1), sum(1 + 1 + n), sum(1 + n - 1), sum(1 + 1 - n) FROM (SELECT number n FROM numbers(10));
SELECT sum(1 + n * 2), sum(1 + 2 * n), sum(1 + n / 2), sum(1 + 1 / n) FROM (SELECT number n FROM numbers(10));
SELECT min(1 + n + 1), min(1 + 1 + n), min(1 + n - 1), min(1 + 1 - n) FROM (SELECT number n FROM numbers(10));
SELECT min(1 + n * 2), min(1 + 2 * n), min(1 + n / 2), min(1 + 1 / n) FROM (SELECT number n FROM numbers(10));
SELECT max(1 + n + 1), max(1 + 1 + n), max(1 + n - 1), max(1 + 1 - n) FROM (SELECT number n FROM numbers(10));
SELECT max(1 + n * 2), max(1 + 2 * n), max(1 + n / 2), max(1 + 1 / n) FROM (SELECT number n FROM numbers(10));

SELECT sum(n + -1 + -1), sum(-1 + n + -1), sum(n - -1 + -1), sum(-1 - n + -1) FROM (SELECT number n FROM numbers(10));
SELECT sum(n * -2 * -1), sum(-2 * n * -1), sum(n / -2 / -1), sum(-1 / n / -1) FROM (SELECT number n FROM numbers(10));
SELECT min(n + -1 + -1), min(-1 + n + -1), min(n - -1 + -1), min(-1 - n + -1) FROM (SELECT number n FROM numbers(10));
SELECT min(n * -2 * -1), min(-2 * n * -1), min(n / -2 / -1), min(-1 / n / -1) FROM (SELECT number n FROM numbers(10));
SELECT max(n + -1 + -1), max(-1 + n + -1), max(n - -1 + -1), max(-1 - n + -1) FROM (SELECT number n FROM numbers(10));
SELECT max(n * -2 * -1), max(-2 * n * -1), max(n / -2 / -1), max(-1 / n / -1) FROM (SELECT number n FROM numbers(10));

SELECT sum(n + 1) + sum(1 + n) + sum(n - 1) + sum(1 - n) FROM (SELECT number n FROM numbers(10));
SELECT sum(n * 2) + sum(2 * n) + sum(n / 2) + sum(1 / n) FROM (SELECT number n FROM numbers(10));
SELECT min(n + 1) + min(1 + n) + min(n - 1) + min(1 - n) FROM (SELECT number n FROM numbers(10));
SELECT min(n * 2) + min(2 * n) + min(n / 2) + min(1 / n) FROM (SELECT number n FROM numbers(10));
SELECT max(n + 1) + max(1 + n) + max(n - 1) + max(1 - n) FROM (SELECT number n FROM numbers(10));
SELECT max(n * 2) + max(2 * n) + max(n / 2) + max(1 / n) FROM (SELECT number n FROM numbers(10));


SELECT sum(number * -3) + min(2 * number * -3) - max(-1 * -2 * number * -3) FROM numbers(100);
SELECT max(log(2) * number) FROM numbers(100);
SELECT round(max(log(2) * 3 * sin(0.3) * number * 4)) FROM numbers(100);
