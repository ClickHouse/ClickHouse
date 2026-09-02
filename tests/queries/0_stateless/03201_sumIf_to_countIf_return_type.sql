SET enable_analyzer = 1;
EXPLAIN QUERY TREE SELECT tuple(sumIf(toInt64(1), 1)) FROM numbers(100) settings optimize_rewrite_sum_if_to_count_if=1;
