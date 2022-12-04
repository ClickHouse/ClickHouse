-- Tags: no-parallel

CREATE FUNCTION _02103_test_function AS x -> x + 1;
CREATE FUNCTION _02103_test_function_with_nested_function_empty_args AS () -> _02103_test_function(1);
CREATE FUNCTION _02103_test_function_with_nested_function_arg AS (x) -> _02103_test_function(x);

SELECT _02103_test_function_with_nested_function_empty_args();
SELECT _02103_test_function_with_nested_function_arg(1);

DROP FUNCTION _02103_test_function_with_nested_function_empty_args;
DROP FUNCTION _02103_test_function_with_nested_function_arg;
DROP FUNCTION _02103_test_function;
