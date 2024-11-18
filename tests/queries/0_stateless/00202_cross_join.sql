SELECT x, y FROM (SELECT number AS x FROM system.numbers LIMIT 3) js1 CROSS JOIN (SELECT number AS y FROM system.numbers LIMIT 5) js2;

SET join_algorithm = 'auto';
SELECT x, y FROM (SELECT number AS x FROM system.numbers LIMIT 3) js1 CROSS JOIN (SELECT number AS y FROM system.numbers LIMIT 5) js2;

-- Just to test that we preserved old setting name this we use `enable_analyzer` instead of `enable_analyzer` here.
SET allow_experimental_analyzer = 1;
SELECT x, y FROM (SELECT number AS x FROM system.numbers LIMIT 3) js1 CROSS JOIN (SELECT number AS y FROM system.numbers LIMIT 5) js2;
