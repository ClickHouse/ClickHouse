SELECT x, y FROM (SELECT number AS x FROM system.numbers LIMIT 3) js1 CROSS JOIN (SELECT number AS y FROM system.numbers LIMIT 5) js2 ORDER BY x, y;

SET join_algorithm = 'auto';
SELECT x, y FROM (SELECT number AS x FROM system.numbers LIMIT 3) js1 CROSS JOIN (SELECT number AS y FROM system.numbers LIMIT 5) js2 ORDER BY x, y;

-- Just to test that we preserved old setting name this we use `enable_analyzer` instead of `enable_analyzer` here.
SET enable_analyzer = 1;
SELECT x, y FROM (SELECT number AS x FROM system.numbers LIMIT 3) js1 CROSS JOIN (SELECT number AS y FROM system.numbers LIMIT 5) js2 ORDER BY x, y;
