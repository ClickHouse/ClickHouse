-- Tags: no-parallel

CREATE FUNCTION _02101_test_function AS x -> x + 1;

SELECT _02101_test_function(1);

DROP FUNCTION _02101_test_function;
DROP FUNCTION _02101_test_function; --{serverError 46}
DROP FUNCTION IF EXISTS _02101_test_function;
