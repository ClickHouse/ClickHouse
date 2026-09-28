-- https://github.com/ClickHouse/ClickHouse/issues/78074
SELECT 1 FROM (SELECT 1 c0) ty JOIN (SELECT 1 c0) tx ON FALSE SETTINGS allow_experimental_join_right_table_sorting = 1, query_plan_use_logical_join_step = 0, join_to_sort_minimum_perkey_rows = 0;
