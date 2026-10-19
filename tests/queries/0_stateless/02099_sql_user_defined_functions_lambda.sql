-- Tags: no-parallel
-- Tag no-parallel: fixed global SQL UDF name, concurrent flaky-check reruns of this file race on CREATE/DROP FUNCTION
CREATE FUNCTION 02099_lambda_function AS x -> arrayMap(array_element -> array_element * 2, x);
SELECT 02099_lambda_function([1,2,3]);
DROP FUNCTION 02099_lambda_function;
