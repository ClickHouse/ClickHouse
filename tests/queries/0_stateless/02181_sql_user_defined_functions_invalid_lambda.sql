CREATE FUNCTION 02181_invalid_lambda AS lambda(((x * 2) AS x_doubled) + x_doubled); --{serverError 1}
CREATE FUNCTION 02181_invalid_lambda AS lambda(x); --{serverError 1}
CREATE FUNCTION 02181_invalid_lambda AS lambda(); --{serverError 1}
CREATE FUNCTION 02181_invalid_lambda AS lambda(tuple(x)) --{serverError 1}
