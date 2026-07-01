SET query_plan_join_swap_table = 0;
SET enable_parallel_replicas = 0;
SET joined_subquery_requires_alias = 0;

-- Some joins return different results with and without the analyzer. Let's enforce both stay the same.
SET enable_analyzer = 0;
SELECT 'cross_true';
SELECT l, r
FROM values('l UInt8', 1, 2) AS l_tbl
CROSS JOIN values('r UInt8', 10, 20) AS r_tbl
ORDER BY l, r;

SELECT 'comma_true';
SELECT l, r
FROM values('l UInt8', 1, 2) AS l_tbl, values('r UInt8', 10, 20) AS r_tbl
ORDER BY l, r;

SELECT 'all_inner_true';
SELECT l, r
FROM values('l UInt8', 1, 2) AS l_tbl
ALL INNER JOIN values('r UInt8', 10, 20) AS r_tbl ON 1
ORDER BY l, r;

SELECT 'all_inner_false';
SELECT l, r
FROM values('l UInt8', 1, 2) AS l_tbl
ALL INNER JOIN values('r UInt8', 10, 20) AS r_tbl ON 0
ORDER BY l, r;

SELECT 'all_left_true';
SELECT l, r
FROM values('l UInt8', 1, 2) AS l_tbl
ALL LEFT JOIN values('r UInt8', 10, 20) AS r_tbl ON 1
ORDER BY l, r;

SELECT 'all_left_false';
SELECT l, r
FROM values('l UInt8', 1, 2) AS l_tbl
ALL LEFT JOIN values('r UInt8', 10, 20) AS r_tbl ON 0
ORDER BY l, r;

SELECT 'all_right_true';
SELECT l, r
FROM values('l UInt8', 1, 2) AS l_tbl
ALL RIGHT JOIN values('r UInt8', 10, 20) AS r_tbl ON 1
ORDER BY l, r;

SELECT 'all_right_false';
SELECT l, r
FROM values('l UInt8', 1, 2) AS l_tbl
ALL RIGHT JOIN values('r UInt8', 10, 20) AS r_tbl ON 0
ORDER BY l, r;

SELECT 'all_full_true';
SELECT l, r
FROM values('l UInt8', 1, 2) AS l_tbl
ALL FULL JOIN values('r UInt8', 10, 20) AS r_tbl ON 1
ORDER BY l, r;

SELECT 'all_full_false';
SELECT l, r
FROM values('l UInt8', 1, 2) AS l_tbl
ALL FULL JOIN values('r UInt8', 10, 20) AS r_tbl ON 0
ORDER BY l, r;

SELECT 'any_inner_true';
SELECT l, r
FROM values('l UInt8', 1, 2) AS l_tbl
ANY INNER JOIN values('r UInt8', 10, 20) AS r_tbl ON 1
ORDER BY l, r;

SELECT 'any_inner_true_take_last';
SELECT l, r
FROM values('l UInt8', 1, 2) AS l_tbl
ANY INNER JOIN values('r UInt8', 10, 20) AS r_tbl ON 1
ORDER BY l, r SETTINGS join_any_take_last_row = 1;

SELECT 'any_inner_false';
SELECT l, r
FROM values('l UInt8', 1, 2) AS l_tbl
ANY INNER JOIN values('r UInt8', 10, 20) AS r_tbl ON 0
ORDER BY l, r;

SELECT 'any_left_true';
SELECT l, r
FROM values('l UInt8', 1, 2) AS l_tbl
ANY LEFT JOIN values('r UInt8', 10, 20) AS r_tbl ON 1
ORDER BY l, r;

SELECT 'any_left_true_take_last';
SELECT l, r
FROM values('l UInt8', 1, 2) AS l_tbl
ANY LEFT JOIN values('r UInt8', 10, 20) AS r_tbl ON 1
ORDER BY l, r SETTINGS join_any_take_last_row = 1;

SELECT 'any_left_false';
SELECT l, r
FROM values('l UInt8', 1, 2) AS l_tbl
ANY LEFT JOIN values('r UInt8', 10, 20) AS r_tbl ON 0
ORDER BY l, r;

SELECT 'any_right_true';
SELECT l, r
FROM values('l UInt8', 1, 2) AS l_tbl
ANY RIGHT JOIN values('r UInt8', 10, 20) AS r_tbl ON 1
ORDER BY l, r;

SELECT 'any_right_true_take_last';
SELECT l, r
FROM values('l UInt8', 1, 2) AS l_tbl
ANY RIGHT JOIN values('r UInt8', 10, 20) AS r_tbl ON 1
ORDER BY l, r SETTINGS join_any_take_last_row = 1;

SELECT 'any_right_false';
SELECT l, r
FROM values('l UInt8', 1, 2) AS l_tbl
ANY RIGHT JOIN values('r UInt8', 10, 20) AS r_tbl ON 0
ORDER BY l, r;

SET any_join_distinct_right_table_keys = 1;

SELECT 'rightany_left_true';
SELECT l, r
FROM values('l UInt8', 1, 2) AS l_tbl
ANY LEFT JOIN values('r UInt8', 10, 20) AS r_tbl ON 1
ORDER BY l, r;

SELECT 'rightany_left_true_take_last';
SELECT l, r
FROM values('l UInt8', 1, 2) AS l_tbl
ANY LEFT JOIN values('r UInt8', 10, 20) AS r_tbl ON 1
ORDER BY l, r SETTINGS join_any_take_last_row = 1;

SELECT 'rightany_right_true';
SELECT l, r
FROM values('l UInt8', 1, 2) AS l_tbl
ANY RIGHT JOIN values('r UInt8', 10, 20) AS r_tbl ON 1
ORDER BY l, r;

SELECT 'rightany_right_false';
SELECT l, r
FROM values('l UInt8', 1, 2) AS l_tbl
ANY RIGHT JOIN values('r UInt8', 10, 20) AS r_tbl ON 0
ORDER BY l, r;

SELECT 'rightany_full_true';
SELECT l, r
FROM values('l UInt8', 1, 2) AS l_tbl
ANY FULL JOIN values('r UInt8', 10, 20) AS r_tbl ON 1
ORDER BY l, r;

SELECT 'rightany_full_false';
SELECT l, r
FROM values('l UInt8', 1, 2) AS l_tbl
ANY FULL JOIN values('r UInt8', 10, 20) AS r_tbl ON 0
ORDER BY l, r;

SET any_join_distinct_right_table_keys = 0;

SELECT 'left_semi_true';
SELECT l, r
FROM values('l UInt8', 1, 2) AS l_tbl
LEFT SEMI JOIN values('r UInt8', 10, 20) AS r_tbl ON 1
ORDER BY l, r;

SELECT 'left_semi_true_take_last';
SELECT l, r
FROM values('l UInt8', 1, 2) AS l_tbl
LEFT SEMI JOIN values('r UInt8', 10, 20) AS r_tbl ON 1
ORDER BY l, r SETTINGS join_any_take_last_row = 1;

SELECT 'left_semi_false';
SELECT l, r
FROM values('l UInt8', 1, 2) AS l_tbl
LEFT SEMI JOIN values('r UInt8', 10, 20) AS r_tbl ON 0
ORDER BY l, r;

SELECT 'right_semi_true';
SELECT l, r
FROM values('l UInt8', 1, 2) AS l_tbl
RIGHT SEMI JOIN values('r UInt8', 10, 20) AS r_tbl ON 1
ORDER BY l, r;

SELECT 'right_semi_false';
SELECT l, r
FROM values('l UInt8', 1, 2) AS l_tbl
RIGHT SEMI JOIN values('r UInt8', 10, 20) AS r_tbl ON 0
ORDER BY l, r;

SELECT 'left_anti_true';
SELECT l, r
FROM values('l UInt8', 1, 2) AS l_tbl
LEFT ANTI JOIN values('r UInt8', 10, 20) AS r_tbl ON 1
ORDER BY l, r;

SELECT 'left_anti_false';
SELECT l, r
FROM values('l UInt8', 1, 2) AS l_tbl
LEFT ANTI JOIN values('r UInt8', 10, 20) AS r_tbl ON 0
ORDER BY l, r;

SELECT 'right_anti_true';
SELECT l, r
FROM values('l UInt8', 1, 2) AS l_tbl
RIGHT ANTI JOIN values('r UInt8', 10, 20) AS r_tbl ON 1
ORDER BY l, r;

SELECT 'right_anti_false';
SELECT l, r
FROM values('l UInt8', 1, 2) AS l_tbl
RIGHT ANTI JOIN values('r UInt8', 10, 20) AS r_tbl ON 0
ORDER BY l, r;

SELECT 'any_left_empty_right';
SELECT l, r
FROM values('l UInt8', 1, 2) AS l_tbl
ANY LEFT JOIN (SELECT toUInt8(number + 10) AS r FROM numbers(0)) AS r_tbl ON 1
ORDER BY l, r;

SELECT 'any_right_empty_left';
SELECT l, r
FROM (SELECT toUInt8(number + 1) AS l FROM numbers(0)) AS l_tbl
ANY RIGHT JOIN values('r UInt8', 10, 20) AS r_tbl ON 1
ORDER BY l, r;

SELECT 'left_semi_empty_right';
SELECT l, r
FROM values('l UInt8', 1, 2) AS l_tbl
LEFT SEMI JOIN (SELECT toUInt8(number + 10) AS r FROM numbers(0)) AS r_tbl ON 1
ORDER BY l, r;

SELECT 'right_semi_empty_left';
SELECT l, r
FROM (SELECT toUInt8(number + 1) AS l FROM numbers(0)) AS l_tbl
RIGHT SEMI JOIN values('r UInt8', 10, 20) AS r_tbl ON 1
ORDER BY l, r;

SELECT 'left_anti_empty_right';
SELECT l, r
FROM values('l UInt8', 1, 2) AS l_tbl
LEFT ANTI JOIN (SELECT toUInt8(number + 10) AS r FROM numbers(0)) AS r_tbl ON 1
ORDER BY l, r;

SELECT 'right_anti_empty_left';
SELECT l, r
FROM (SELECT toUInt8(number + 1) AS l FROM numbers(0)) AS l_tbl
RIGHT ANTI JOIN values('r UInt8', 10, 20) AS r_tbl ON 1
ORDER BY l, r;

-- With analyzer
SET enable_analyzer = 1;
SELECT 'cross_true';
SELECT l, r
FROM values('l UInt8', 1, 2) AS l_tbl
CROSS JOIN values('r UInt8', 10, 20) AS r_tbl
ORDER BY l, r;

SELECT 'comma_true';
SELECT l, r
FROM values('l UInt8', 1, 2) AS l_tbl, values('r UInt8', 10, 20) AS r_tbl
ORDER BY l, r;

SELECT 'all_inner_true';
SELECT l, r
FROM values('l UInt8', 1, 2) AS l_tbl
ALL INNER JOIN values('r UInt8', 10, 20) AS r_tbl ON 1
ORDER BY l, r;

SELECT 'all_inner_false';
SELECT l, r
FROM values('l UInt8', 1, 2) AS l_tbl
ALL INNER JOIN values('r UInt8', 10, 20) AS r_tbl ON 0
ORDER BY l, r;

SELECT 'all_left_true';
SELECT l, r
FROM values('l UInt8', 1, 2) AS l_tbl
ALL LEFT JOIN values('r UInt8', 10, 20) AS r_tbl ON 1
ORDER BY l, r;

SELECT 'all_left_false';
SELECT l, r
FROM values('l UInt8', 1, 2) AS l_tbl
ALL LEFT JOIN values('r UInt8', 10, 20) AS r_tbl ON 0
ORDER BY l, r;

SELECT 'all_right_true';
SELECT l, r
FROM values('l UInt8', 1, 2) AS l_tbl
ALL RIGHT JOIN values('r UInt8', 10, 20) AS r_tbl ON 1
ORDER BY l, r;

SELECT 'all_right_false';
SELECT l, r
FROM values('l UInt8', 1, 2) AS l_tbl
ALL RIGHT JOIN values('r UInt8', 10, 20) AS r_tbl ON 0
ORDER BY l, r;

SELECT 'all_full_true';
SELECT l, r
FROM values('l UInt8', 1, 2) AS l_tbl
ALL FULL JOIN values('r UInt8', 10, 20) AS r_tbl ON 1
ORDER BY l, r;

SELECT 'all_full_false';
SELECT l, r
FROM values('l UInt8', 1, 2) AS l_tbl
ALL FULL JOIN values('r UInt8', 10, 20) AS r_tbl ON 0
ORDER BY l, r;

-- TODO(antaljanosbenjamin): remove the comment and the extra settings once the bug is fixed
-- join order convert ANY INNER on constant to CROSS, will be fixed in another PR
SELECT 'any_inner_true';
SELECT l, r
FROM values('l UInt8', 1, 2) AS l_tbl
ANY INNER JOIN values('r UInt8', 10, 20) AS r_tbl ON 1
ORDER BY l, r SETTINGS query_plan_optimize_join_order_limit = 0;

SELECT 'any_inner_true_take_last';
SELECT l, r
FROM values('l UInt8', 1, 2) AS l_tbl
ANY INNER JOIN values('r UInt8', 10, 20) AS r_tbl ON 1
ORDER BY l, r SETTINGS query_plan_optimize_join_order_limit = 0;

SELECT 'any_inner_false';
SELECT l, r
FROM values('l UInt8', 1, 2) AS l_tbl
ANY INNER JOIN values('r UInt8', 10, 20) AS r_tbl ON 0
ORDER BY l, r;

SELECT 'any_left_true';
SELECT l, r
FROM values('l UInt8', 1, 2) AS l_tbl
ANY LEFT JOIN values('r UInt8', 10, 20) AS r_tbl ON 1
ORDER BY l, r;

SELECT 'any_left_true_take_last';
SELECT l, r
FROM values('l UInt8', 1, 2) AS l_tbl
ANY LEFT JOIN values('r UInt8', 10, 20) AS r_tbl ON 1
ORDER BY l, r SETTINGS join_any_take_last_row = 1;

SELECT 'any_left_false';
SELECT l, r
FROM values('l UInt8', 1, 2) AS l_tbl
ANY LEFT JOIN values('r UInt8', 10, 20) AS r_tbl ON 0
ORDER BY l, r;

SELECT 'any_right_true';
SELECT l, r
FROM values('l UInt8', 1, 2) AS l_tbl
ANY RIGHT JOIN values('r UInt8', 10, 20) AS r_tbl ON 1
ORDER BY l, r;

SELECT 'any_right_true_take_last';
SELECT l, r
FROM values('l UInt8', 1, 2) AS l_tbl
ANY RIGHT JOIN values('r UInt8', 10, 20) AS r_tbl ON 1
ORDER BY l, r SETTINGS join_any_take_last_row = 1;

SELECT 'any_right_false';
SELECT l, r
FROM values('l UInt8', 1, 2) AS l_tbl
ANY RIGHT JOIN values('r UInt8', 10, 20) AS r_tbl ON 0
ORDER BY l, r;

SET any_join_distinct_right_table_keys = 1;

SELECT 'rightany_left_true';
SELECT l, r
FROM values('l UInt8', 1, 2) AS l_tbl
ANY LEFT JOIN values('r UInt8', 10, 20) AS r_tbl ON 1
ORDER BY l, r;

SELECT 'rightany_left_true_take_last';
SELECT l, r
FROM values('l UInt8', 1, 2) AS l_tbl
ANY LEFT JOIN values('r UInt8', 10, 20) AS r_tbl ON 1
ORDER BY l, r SETTINGS join_any_take_last_row = 1;

SELECT 'rightany_right_true';
SELECT l, r
FROM values('l UInt8', 1, 2) AS l_tbl
ANY RIGHT JOIN values('r UInt8', 10, 20) AS r_tbl ON 1
ORDER BY l, r;

SELECT 'rightany_right_false';
SELECT l, r
FROM values('l UInt8', 1, 2) AS l_tbl
ANY RIGHT JOIN values('r UInt8', 10, 20) AS r_tbl ON 0
ORDER BY l, r;

SELECT 'rightany_full_true';
SELECT l, r
FROM values('l UInt8', 1, 2) AS l_tbl
ANY FULL JOIN values('r UInt8', 10, 20) AS r_tbl ON 1
ORDER BY l, r;

SELECT 'rightany_full_false';
SELECT l, r
FROM values('l UInt8', 1, 2) AS l_tbl
ANY FULL JOIN values('r UInt8', 10, 20) AS r_tbl ON 0
ORDER BY l, r;

SET any_join_distinct_right_table_keys = 0;

SELECT 'left_semi_true';
SELECT l, r
FROM values('l UInt8', 1, 2) AS l_tbl
LEFT SEMI JOIN values('r UInt8', 10, 20) AS r_tbl ON 1
ORDER BY l, r;

SELECT 'left_semi_true_take_last';
SELECT l, r
FROM values('l UInt8', 1, 2) AS l_tbl
LEFT SEMI JOIN values('r UInt8', 10, 20) AS r_tbl ON 1
ORDER BY l, r SETTINGS join_any_take_last_row = 1;

SELECT 'left_semi_false';
SELECT l, r
FROM values('l UInt8', 1, 2) AS l_tbl
LEFT SEMI JOIN values('r UInt8', 10, 20) AS r_tbl ON 0
ORDER BY l, r;

SELECT 'right_semi_true';
SELECT l, r
FROM values('l UInt8', 1, 2) AS l_tbl
RIGHT SEMI JOIN values('r UInt8', 10, 20) AS r_tbl ON 1
ORDER BY l, r;

SELECT 'right_semi_false';
SELECT l, r
FROM values('l UInt8', 1, 2) AS l_tbl
RIGHT SEMI JOIN values('r UInt8', 10, 20) AS r_tbl ON 0
ORDER BY l, r;

SELECT 'left_anti_true';
SELECT l, r
FROM values('l UInt8', 1, 2) AS l_tbl
LEFT ANTI JOIN values('r UInt8', 10, 20) AS r_tbl ON 1
ORDER BY l, r;

SELECT 'left_anti_false';
SELECT l, r
FROM values('l UInt8', 1, 2) AS l_tbl
LEFT ANTI JOIN values('r UInt8', 10, 20) AS r_tbl ON 0
ORDER BY l, r;

SELECT 'right_anti_true';
SELECT l, r
FROM values('l UInt8', 1, 2) AS l_tbl
RIGHT ANTI JOIN values('r UInt8', 10, 20) AS r_tbl ON 1
ORDER BY l, r;

SELECT 'right_anti_false';
SELECT l, r
FROM values('l UInt8', 1, 2) AS l_tbl
RIGHT ANTI JOIN values('r UInt8', 10, 20) AS r_tbl ON 0
ORDER BY l, r;

SELECT 'any_left_empty_right';
SELECT l, r
FROM values('l UInt8', 1, 2) AS l_tbl
ANY LEFT JOIN (SELECT toUInt8(number + 10) AS r FROM numbers(0)) AS r_tbl ON 1
ORDER BY l, r;

SELECT 'any_right_empty_left';
SELECT l, r
FROM (SELECT toUInt8(number + 1) AS l FROM numbers(0)) AS l_tbl
ANY RIGHT JOIN values('r UInt8', 10, 20) AS r_tbl ON 1
ORDER BY l, r;

SELECT 'left_semi_empty_right';
SELECT l, r
FROM values('l UInt8', 1, 2) AS l_tbl
LEFT SEMI JOIN (SELECT toUInt8(number + 10) AS r FROM numbers(0)) AS r_tbl ON 1
ORDER BY l, r;

SELECT 'right_semi_empty_left';
SELECT l, r
FROM (SELECT toUInt8(number + 1) AS l FROM numbers(0)) AS l_tbl
RIGHT SEMI JOIN values('r UInt8', 10, 20) AS r_tbl ON 1
ORDER BY l, r;

SELECT 'left_anti_empty_right';
SELECT l, r
FROM values('l UInt8', 1, 2) AS l_tbl
LEFT ANTI JOIN (SELECT toUInt8(number + 10) AS r FROM numbers(0)) AS r_tbl ON 1
ORDER BY l, r;

SELECT 'right_anti_empty_left';
SELECT l, r
FROM (SELECT toUInt8(number + 1) AS l FROM numbers(0)) AS l_tbl
RIGHT ANTI JOIN values('r UInt8', 10, 20) AS r_tbl ON 1
ORDER BY l, r;
