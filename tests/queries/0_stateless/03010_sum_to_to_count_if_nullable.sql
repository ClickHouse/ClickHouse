SET optimize_rewrite_sum_if_to_count_if = 1;

SET allow_experimental_analyzer = 0;
SELECT (sumIf(toNullable(1), (number % 2) = 0), NULL) FROM numbers(10);
SELECT (sum(if((number % 2) = 0, toNullable(1), 0)), NULL) FROM numbers(10);

SET allow_experimental_analyzer = 1;
SELECT (sumIf(toNullable(1), (number % 2) = 0), NULL) FROM numbers(10);
EXPLAIN QUERY TREE SELECT (sumIf(toNullable(1), (number % 2) = 0), NULL) FROM numbers(10);
SELECT (sum(if((number % 2) = 0, toNullable(1), 0)), NULL) FROM numbers(10);
EXPLAIN QUERY TREE SELECT (sum(if((number % 2) = 0, toNullable(1), 0)), NULL) FROM numbers(10);