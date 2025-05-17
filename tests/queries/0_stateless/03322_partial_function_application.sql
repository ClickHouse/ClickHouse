-- Tags: no-parallel

SELECT arrayMap(plus(_1, _2), [1, 2, 3], [4, 5, 6]);
SELECT arrayMap(plus(5, _), [1, 3, 5]);
SELECT arrayMap(plus(_1, _1), [1, 3, 5], [4, 5, 6]); -- { clientError UNKNOWN_IDENTIFIER }
SELECT arrayMap(plus(5, _3), [1, 3, 5]); -- { clientError UNKNOWN_IDENTIFIER }
SELECT arrayMap(plus(5, _3x), [1, 3, 5]); -- { clientError UNKNOWN_IDENTIFIER }

DROP FUNCTION IF EXISTS 03322_linear_function;
CREATE FUNCTION 03322_linear_function AS (x, k, b) -> k * x + b;

SELECT arrayMap(03322_linear_function(_, 3, 5), [4, 6, 7]);
SELECT arrayMap(03322_linear_function(10, _2, _1), [4, 6, 7], [10, 3, -1]);
SELECT arrayMap(03322_linear_function(_3, _1, _2), [4, 6, 7], [10, 3, -1], [1, 4, 0]);
SELECT arrayMap(03322_linear_function(_4, _1, _2), [4, 6, 7], [10, 3, -1], [1, 4, 0]); -- { clientError UNKNOWN_IDENTIFIER }

SELECT arrayMap(pow(_2, _1), [3, 2, 1], [9, 10, 11]);

DROP FUNCTION 03322_linear_function;
