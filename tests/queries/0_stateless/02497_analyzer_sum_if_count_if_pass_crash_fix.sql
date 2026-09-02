SET enable_analyzer = 1;
SET optimize_rewrite_sum_if_to_count_if = 1;

SELECT sum(if((number % 2) = 0 AS cond_expr, 1 AS one_expr, 0 AS zero_expr) AS if_expr), sum(cond_expr), sum(if_expr), one_expr, zero_expr FROM numbers(100);
