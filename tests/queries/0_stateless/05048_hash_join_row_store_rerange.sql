-- Row store must stay disabled when right-table rerange can fire. Constructor used to
-- call initRowStore before maps.resize, so maps.size()==0 skipped that check.

SELECT r.p1, r.p2
FROM (SELECT number % 4 AS k FROM numbers(8)) AS l
INNER JOIN (SELECT number % 2 AS k, number AS p1, number + 10 AS p2 FROM numbers(8)) AS r
ON l.k = r.k
ORDER BY r.p1, r.p2
SETTINGS join_algorithm = 'hash', enable_hash_join_row_store = 1, min_rows_ratio_for_hash_join_row_store = 0, allow_experimental_join_right_table_sorting = 1, join_to_sort_minimum_perkey_rows = 0, join_to_sort_maximum_table_rows = 10000, query_plan_join_swap_table = 0;
