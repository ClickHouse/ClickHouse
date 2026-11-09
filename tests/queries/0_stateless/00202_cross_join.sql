SELECT x, y FROM (SELECT number AS x FROM system.numbers LIMIT 3) js1 CROSS JOIN (SELECT number AS y FROM system.numbers LIMIT 5) js2 ORDER BY x, y;

SET join_algorithm = 'auto';
SELECT x, y FROM (SELECT number AS x FROM system.numbers LIMIT 3) js1 CROSS JOIN (SELECT number AS y FROM system.numbers LIMIT 5) js2 ORDER BY x, y;

SELECT x, y FROM (SELECT number AS x FROM system.numbers LIMIT 3) js1 CROSS JOIN (SELECT number AS y FROM system.numbers LIMIT 5) js2 ORDER BY x, y;
