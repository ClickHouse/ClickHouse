-- Tags: no-parallel
CREATE FUNCTION lambda_function AS x -> arrayMap(array_element -> array_element * 2, x);
SELECT lambda_function([1,2,3]);
DROP FUNCTION lambda_function;
