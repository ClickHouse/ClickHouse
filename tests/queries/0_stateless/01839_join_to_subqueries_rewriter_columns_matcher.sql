SELECT COLUMNS('test') FROM
    (SELECT 1 AS id, 'a' AS test) a
    LEFT JOIN (SELECT 1 AS id, 'b' AS test) b ON b.id = a.id
    LEFT JOIN (SELECT 1 AS id, 'c' AS test) c ON c.id = a.id;
