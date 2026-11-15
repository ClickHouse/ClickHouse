select sum(if((number % NULL) = 2, 0, 1)) FROM numbers(1024) settings optimize_rewrite_sum_if_to_count_if=0;
select sum(if((number % NULL) = 2, 0, 1)) FROM numbers(1024) settings optimize_rewrite_sum_if_to_count_if=1, enable_analyzer=0;
select sum(if((number % NULL) = 2, 0, 1)) FROM numbers(1024) settings optimize_rewrite_sum_if_to_count_if=1, enable_analyzer=1;
