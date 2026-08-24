SELECT x FROM (SELECT 1 AS x, 2 AS y) LIMIT 1 BY y;
SELECT x FROM (SELECT number AS x, number + 1 AS y FROM system.numbers LIMIT 10) ORDER BY y LIMIT 1 BY y;
SELECT sum(x) FROM (SELECT x, y FROM (SELECT number AS x, number + 1 AS y FROM system.numbers LIMIT 10) ORDER BY y LIMIT 1 BY y);
