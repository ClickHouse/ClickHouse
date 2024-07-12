SELECT x, y FROM (SELECT number AS x FROM system.numbers LIMIT 3) js1 CROSS JOIN (SELECT number AS y FROM system.numbers LIMIT 5) js2;

SET join_algorithm = 'auto';
SELECT x, y FROM (SELECT number AS x FROM system.numbers LIMIT 3) js1 CROSS JOIN (SELECT number AS y FROM system.numbers LIMIT 5) js2;

SET allow_experimental_analyzer = 1;
SELECT x, y FROM (SELECT number AS x FROM system.numbers LIMIT 3) js1 CROSS JOIN (SELECT number AS y FROM system.numbers LIMIT 5) js2;

SELECT * FROM ( SELECT 1 AS a, toLowCardinality(1), 1) AS t1 CROSS  JOIN (SELECT toLowCardinality(1 AS a), 1 AS b) AS t2;
