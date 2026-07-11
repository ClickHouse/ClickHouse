-- Tags: no-parallel
SELECT countDistinctIf(number % 10, number % 5 = 2) FROM numbers(1000);
EXPLAIN SYNTAX SELECT countDistinctIf(number % 10, number % 5 = 2) FROM numbers(1000);

-- disable by default
SET rewrite_count_distinct_if_with_count_distinct_implementation = 1;
SELECT countDistinctIf(number % 10, number % 5 = 2) FROM numbers(1000);
EXPLAIN SYNTAX SELECT countDistinctIf(number % 10, number % 5 = 2) FROM numbers(1000);
