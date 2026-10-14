-- Tags: no-parallel
-- Tag no-parallel: fixed global SQL UDF name, concurrent flaky-check reruns of this file race on CREATE/DROP FUNCTION
CREATE FUNCTION 02096_test_function AS x -> x + 1;
DESCRIBE (SELECT 02096_test_function(1) AS a);
DROP FUNCTION 02096_test_function;
