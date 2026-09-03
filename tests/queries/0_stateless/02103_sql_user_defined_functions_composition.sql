-- Tags: no-parallel

CREATE FUNCTION 02103_test_function AS x -> x + 1;
CREATE FUNCTION 02103_test_function_with_nested_function_empty_args AS () -> 02103_test_function(1);
CREATE FUNCTION 02103_test_function_with_nested_function_arg AS (x) -> 02103_test_function(x);

SELECT 02103_test_function_with_nested_function_empty_args();
SELECT 02103_test_function_with_nested_function_arg(1);

DROP FUNCTION 02103_test_function_with_nested_function_empty_args;
DROP FUNCTION 02103_test_function_with_nested_function_arg;
DROP FUNCTION 02103_test_function;
