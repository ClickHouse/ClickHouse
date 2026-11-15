SET any_join_distinct_right_table_keys = 1;

SELECT a.*, b.* FROM
(
    SELECT number AS k FROM system.numbers LIMIT 10
) AS a
ANY INNER JOIN
(
    SELECT number * 2 AS k, number AS joined FROM system.numbers LIMIT 10
) AS b
USING k
ORDER BY ALL;
