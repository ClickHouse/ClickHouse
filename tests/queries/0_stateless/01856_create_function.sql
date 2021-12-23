-- Tags: no-parallel

CREATE FUNCTION 01856_test_function_0 AS (a, b, c) -> a * b * c;
SELECT 01856_test_function_0(2, 3, 4);
SELECT isConstant(01856_test_function_0(1, 2, 3));
DROP FUNCTION 01856_test_function_0;
CREATE FUNCTION 01856_test_function_1 AS (a, b) -> 01856_test_function_1(a, b) + 01856_test_function_1(a, b); --{serverError 611}
CREATE FUNCTION cast AS a -> a + 1; --{serverError 609}
CREATE FUNCTION sum AS (a, b) -> a + b; --{serverError 609}
CREATE FUNCTION 01856_test_function_2 AS (a, b) -> a + b;
CREATE FUNCTION 01856_test_function_2 AS (a) -> a || '!!!'; --{serverError 609}
DROP FUNCTION 01856_test_function_2;
DROP FUNCTION unknown_function; -- {serverError 46}
DROP FUNCTION CAST; -- {serverError 610}
