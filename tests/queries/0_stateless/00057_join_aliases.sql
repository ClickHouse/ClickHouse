SET query_plan_join_swap_table = 0;

SELECT * FROM (
    SELECT number, n, j1, j2
    FROM (SELECT number, number / 2 AS n FROM system.numbers) js1
    ANY LEFT JOIN (SELECT number / 3 AS n, number AS j1, 'Hello' AS j2 FROM system.numbers LIMIT 10) js2
    USING n LIMIT 10
) ORDER BY n
SETTINGS join_algorithm = 'hash'; -- the query does not finish with merge join
