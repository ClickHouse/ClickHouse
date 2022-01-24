-- Tags: no-parallel
CREATE FUNCTION 02098_alias_function AS x -> (((x * 2) AS x_doubled) + x_doubled);
SELECT 02098_alias_function(2);
DROP FUNCTION 02098_alias_function;
