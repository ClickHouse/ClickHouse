CREATE FUNCTION 02181_invalid_lambda AS lambda(((x * 2) AS x_doubled) + x_doubled); --{serverError BAD_ARGUMENTS}
CREATE FUNCTION 02181_invalid_lambda AS lambda(x); --{serverError BAD_ARGUMENTS}
CREATE FUNCTION 02181_invalid_lambda AS lambda(); --{serverError BAD_ARGUMENTS}
CREATE FUNCTION 02181_invalid_lambda AS lambda(tuple(x)); --{serverError BAD_ARGUMENTS}
CREATE FUNCTION 02181_invalid_lambda AS lambda(tuple(x.y), 1); --{serverError BAD_ARGUMENTS}
CREATE FUNCTION 02181_invalid_lambda AS (x.y) -> 1; --{serverError BAD_ARGUMENTS}
CREATE FUNCTION 02181_invalid_lambda AS lambda(tuple(a.b.c), 1); --{serverError BAD_ARGUMENTS}
CREATE FUNCTION 02181_invalid_lambda AS lambda(tuple(x, y.z), 1); --{serverError BAD_ARGUMENTS}
CREATE FUNCTION 02181_dotted_name AS lambda(tuple(`x.y`), 02181_dotted_name(1) + 1); --{serverError CANNOT_CREATE_RECURSIVE_FUNCTION}
CREATE FUNCTION 02181_invalid_lambda AS lambda(tuple([a, b]), 1); --{serverError BAD_ARGUMENTS}
CREATE FUNCTION 02181_invalid_lambda AS lambda(tuple(map('a', 1)), 1); --{serverError BAD_ARGUMENTS}
CREATE FUNCTION 02181_invalid_lambda AS lambda([x], x + 1); --{serverError BAD_ARGUMENTS}
CREATE FUNCTION 02181_invalid_lambda AS (x) -> arrayMap(y.z -> y.z + x, [1, 2]); --{serverError BAD_ARGUMENTS}
CREATE FUNCTION 02181_invalid_lambda AS (x) -> mapApply((k.a, v) -> (k.a, v), map('a', 1)); --{serverError BAD_ARGUMENTS}
CREATE FUNCTION 02181_invalid_lambda AS (x) -> (y.z -> y.z + x); --{serverError BAD_ARGUMENTS}
CREATE FUNCTION 02181_nested_lambda AS (x) -> arrayMap(x -> 02181_nested_lambda(x) + 1, [1, 2]); --{serverError CANNOT_CREATE_RECURSIVE_FUNCTION}
