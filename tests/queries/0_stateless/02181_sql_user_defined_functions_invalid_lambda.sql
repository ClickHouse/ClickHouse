CREATE FUNCTION 02181_invalid_lambda AS lambda(((x * 2) AS x_doubled) + x_doubled); --{serverError 1}
