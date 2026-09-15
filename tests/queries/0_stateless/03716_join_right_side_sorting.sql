SELECT 1
FROM
(
    SELECT 1 AS x
) AS x
LEFT JOIN
(
    SELECT 2 AS y
) AS y ON x.x = y.y
INNER JOIN
(
    SELECT number
    FROM numbers(40)
) AS z ON 1
SETTINGS join_algorithm = 'hash', join_output_by_rowlist_perkey_rows_threshold = 80, allow_experimental_join_right_table_sorting = 1, allow_experimental_parallel_reading_from_replicas = 1;
