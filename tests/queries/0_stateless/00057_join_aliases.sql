SELECT * FROM (
    SELECT number, n, j1, j2
    FROM (SELECT number, number / 2 AS n FROM system.numbers LIMIT 1000) js1
    ANY LEFT JOIN (SELECT number / 3 AS n, number AS j1, 'Hello' AS j2 FROM system.numbers LIMIT 10) js2
    USING n
    ORDER BY number LIMIT 10
) ORDER BY n
SETTINGS join_algorithm = 'hash'; -- the query does not finish with merge join
