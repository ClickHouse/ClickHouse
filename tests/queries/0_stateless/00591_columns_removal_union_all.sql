SELECT * FROM
(
    SELECT x FROM (SELECT x, y, arrayJoin(z) FROM (SELECT number AS x, number + 1 AS y, [number % 2, number % 3] AS z FROM numbers(10)) UNION ALL SELECT 1, 2, 3)
) ORDER BY x;
