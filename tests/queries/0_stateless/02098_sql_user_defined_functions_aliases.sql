-- Tags: no-parallel
CREATE FUNCTION alias_function AS x -> (((x * 2) AS x_doubled) + x_doubled);
SELECT alias_function(2);
DROP FUNCTION alias_function;
