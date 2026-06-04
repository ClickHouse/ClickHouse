SET enable_analyzer = 1;

WITH x -> plus(lambda(1), x) AS lambda SELECT lambda(1048576); -- { serverError UNSUPPORTED_METHOD };

WITH lambda(lambda(plus(x, x, -1)), tuple(x), x + 2147483646) AS lambda, x -> plus(lambda(1), x, 2) AS lambda SELECT 1048576, lambda(1048576); -- { serverError UNSUPPORTED_METHOD };
