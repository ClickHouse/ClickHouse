-- Tags: no-parallel

CREATE FUNCTION 02096_test_function AS x -> x + 1;
DESCRIBE (SELECT 02096_test_function(1) AS a);
DROP FUNCTION 02096_test_function;
