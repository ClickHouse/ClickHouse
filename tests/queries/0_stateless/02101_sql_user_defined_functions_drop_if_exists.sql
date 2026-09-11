
CREATE FUNCTION test_02101_drop_if_exists_function AS x -> x + 1;

SELECT test_02101_drop_if_exists_function(1);

DROP FUNCTION test_02101_drop_if_exists_function;
DROP FUNCTION test_02101_drop_if_exists_function; --{serverError UNKNOWN_FUNCTION}
DROP FUNCTION IF EXISTS test_02101_drop_if_exists_function;
