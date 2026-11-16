-- Regression test: calling .front() on empty maps vector in tryRerangeRightTableData (UBSan, #95518)
SELECT count()
FROM (SELECT number AS x FROM numbers(10)) AS a
CROSS JOIN (SELECT number AS y FROM numbers(10)) AS b
SETTINGS join_algorithm = 'hash', allow_experimental_join_right_table_sorting = 1, join_to_sort_minimum_perkey_rows = 0, join_to_sort_maximum_table_rows = 1000;
