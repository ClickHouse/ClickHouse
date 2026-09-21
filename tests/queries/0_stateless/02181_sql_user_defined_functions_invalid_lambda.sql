CREATE FUNCTION 02181_invalid_lambda AS lambda(((x * 2) AS x_doubled) + x_doubled); --{serverError BAD_ARGUMENTS}
CREATE FUNCTION 02181_invalid_lambda AS lambda(x); --{serverError BAD_ARGUMENTS}
CREATE FUNCTION 02181_invalid_lambda AS lambda(); --{serverError BAD_ARGUMENTS}
CREATE FUNCTION 02181_invalid_lambda AS lambda(tuple(x)); --{serverError BAD_ARGUMENTS}
CREATE FUNCTION 02181_invalid_lambda AS lambda(tuple(x.y), 1); --{serverError BAD_ARGUMENTS}
CREATE FUNCTION 02181_invalid_lambda AS (x.y) -> 1; --{serverError BAD_ARGUMENTS}
CREATE FUNCTION 02181_invalid_lambda AS lambda(tuple(a.b.c), 1); --{serverError BAD_ARGUMENTS}
CREATE FUNCTION 02181_invalid_lambda AS lambda(tuple(x, y.z), 1); --{serverError BAD_ARGUMENTS}
CREATE FUNCTION 02181_dotted_name AS lambda(tuple(`x.y`), 02181_dotted_name(1) + 1); --{serverError CANNOT_CREATE_RECURSIVE_FUNCTION}
