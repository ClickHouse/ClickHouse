SET optimize_rewrite_sum_if_to_count_if = 1;
SELECT (sumIf(toNullable(1), (number % 2) = 0), NULL) FROM numbers(10) SETTINGS allow_experimental_analyzer=0;
SELECT (sumIf(toNullable(1), (number % 2) = 0), NULL) FROM numbers(10) SETTINGS allow_experimental_analyzer=1;