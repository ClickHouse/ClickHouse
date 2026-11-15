SELECT * FROM (SELECT 1 AS id, '' AS test) AS a
LEFT JOIN (SELECT test, 1 AS id, NULL AS test) AS b ON b.id = a.id
SETTINGS join_algorithm = 'auto', max_rows_in_join = 1, enable_analyzer = 1
;
