-- Tags: no-parallel
-- Tag no-parallel: fixed global SQL UDF name, concurrent flaky-check reruns of this file race on CREATE/DROP FUNCTION
CREATE FUNCTION 02098_alias_function AS x -> (((x * 2) AS x_doubled) + x_doubled);
SELECT 02098_alias_function(2);
DROP FUNCTION 02098_alias_function;
