-- Tags: no-parallel

CREATE FUNCTION 01856_test_function_0 AS (a, b, c) -> a * b * c;
SELECT 01856_test_function_0(2, 3, 4);
SELECT isConstant(01856_test_function_0(1, 2, 3));
DROP FUNCTION 01856_test_function_0;
CREATE FUNCTION 01856_test_function_1 AS (a, b) -> 01856_test_function_1(a, b) + 01856_test_function_1(a, b); --{serverError CANNOT_CREATE_RECURSIVE_FUNCTION}
CREATE FUNCTION cast AS a -> a + 1; --{serverError FUNCTION_ALREADY_EXISTS}
CREATE FUNCTION sum AS (a, b) -> a + b; --{serverError FUNCTION_ALREADY_EXISTS}
CREATE FUNCTION 01856_test_function_2 AS (a, b) -> a + b;
CREATE FUNCTION 01856_test_function_2 AS (a) -> a || '!!!'; --{serverError FUNCTION_ALREADY_EXISTS}
DROP FUNCTION 01856_test_function_2;
DROP FUNCTION unknown_function; -- {serverError UNKNOWN_FUNCTION}
DROP FUNCTION CAST; -- {serverError CANNOT_DROP_FUNCTION}
