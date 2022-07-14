-- Tags: no-parallel

CREATE FUNCTION _02096_test_function AS x -> x + 1;
DESCRIBE (SELECT _02096_test_function(1) AS a);
DROP FUNCTION _02096_test_function;
