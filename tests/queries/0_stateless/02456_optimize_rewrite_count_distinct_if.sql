SET optimize_rewrite_count_distinct_if = FALSE;
SELECT countDistinctIf(number % 10, number % 5 = 2) FROM numbers_mt(100000000);
EXPLAIN SYNTAX SELECT countDistinctIf(number % 10, number % 5 = 2) FROM numbers_mt(100000000);

SET optimize_rewrite_count_distinct_if = TRUE;
SELECT countDistinctIf(number % 10, number % 5 = 2) FROM numbers_mt(100000000);
EXPLAIN SYNTAX SELECT countDistinctIf(number % 10, number % 5 = 2) FROM numbers_mt(100000000);

