CREATE FUNCTION 02181_invalid_lambda AS lambda(((x * 2) AS x_doubled) + x_doubled); --{serverError BAD_ARGUMENTS}
CREATE FUNCTION 02181_invalid_lambda AS lambda(x); --{serverError BAD_ARGUMENTS}
CREATE FUNCTION 02181_invalid_lambda AS lambda(); --{serverError BAD_ARGUMENTS}
CREATE FUNCTION 02181_invalid_lambda AS lambda(tuple(x)) --{serverError BAD_ARGUMENTS}
