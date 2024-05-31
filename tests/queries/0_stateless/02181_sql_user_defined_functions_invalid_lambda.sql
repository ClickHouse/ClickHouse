CREATE FUNCTION 02181_invalid_lambda AS lambda(((x * 2) AS x_doubled) + x_doubled); --{serverError UNSUPPORTED_METHOD}
CREATE FUNCTION 02181_invalid_lambda AS lambda(x); --{serverError UNSUPPORTED_METHOD}
CREATE FUNCTION 02181_invalid_lambda AS lambda(); --{serverError UNSUPPORTED_METHOD}
CREATE FUNCTION 02181_invalid_lambda AS lambda(tuple(x)) --{serverError UNSUPPORTED_METHOD}
