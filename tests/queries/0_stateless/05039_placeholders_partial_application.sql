-- Tags: no-parallel
-- Reason for no-parallel: this test creates a SQL UDF (`05039_linear`); concurrent
-- runs in flaky check would race on `CREATE FUNCTION` and `DROP FUNCTION` for this
-- global name.

-- Argument placeholders in the lambda position of a higher-order function
-- lift the expression to a lambda: partial application.

-- The placeholders are resolved only in the analyzer.
SET enable_analyzer = 1;

SELECT arrayMap(plus(_1, _2), [1, 2, 3], [4, 5, 6]);
SELECT arrayMap(plus(5, _), [1, 3, 5]);
SELECT arrayMap(concat(_, _), ['a', 'b'], ['x', 'y']);
SELECT arrayMap(pow(_2, _1), [3, 2, 1], [9, 10, 11]);

-- A bare placeholder is the identity lambda.
SELECT arrayMap(_1, [1, 2, 3]);
SELECT arrayMap(_, [1, 2, 3]);
SELECT arrayFilter(_1, [0, 1, 0, 1]);

-- A numbered placeholder can be repeated.
SELECT arrayMap(multiply(_1, _1), [1, 2, 3]);

-- Numbered placeholders work at any depth, including inside operators.
SELECT arrayMap(_1 * 2 + 1, [1, 2, 3]);
SELECT arrayMap(if(_1 > 0, _1, -_1), [-1, 2, -3]);
SELECT arrayFilter(_1 % 2 = 1, [1, 2, 3, 4, 5]);
SELECT arrayFold(_1 + _2, [1, 2, 3], 0::UInt64);

-- The largest placeholder number determines the lambda arity; unused arguments are allowed.
SELECT arrayMap(plus(5, _3), [1, 2, 3], [4, 5, 6], [7, 8, 9]);

-- SQL user defined functions.
DROP FUNCTION IF EXISTS 05039_linear;
CREATE FUNCTION 05039_linear AS (x, k, b) -> k * x + b;
SELECT arrayMap(05039_linear(_, 3, 5), [4, 6, 7]);
SELECT arrayMap(05039_linear(10, _2, _1), [4, 6, 7], [10, 3, -1]);
SELECT arrayMap(05039_linear(_3, _1, _2), [4, 6, 7], [10, 3, -1], [1, 4, 0]);
DROP FUNCTION 05039_linear;

-- Names bound in scope take priority, so no previously valid query changes meaning:
-- `_` as a lambda argument is not a placeholder,
SELECT arrayExists(_ -> NOT (_ IN (1)), [1, 2]);
-- outside the lambda position of a higher-order function `_N` is an ordinary identifier,
SELECT plus(_1, 1) FROM (SELECT 1 AS _1);
SELECT arrayMap(x -> plus(x, _1), [1]) FROM (SELECT 10 AS _1);

-- Malformed placeholders.
SELECT arrayMap(plus(_1, _), [1], [2]); -- { serverError BAD_ARGUMENTS }
SELECT arrayMap(plus(1, multiply(_, 2)), [1]); -- { serverError BAD_ARGUMENTS }
SELECT arrayMap(plus(5, _300), [1]); -- { serverError BAD_ARGUMENTS }
SELECT arrayMap(plus(_0, 1), [1]); -- { serverError UNKNOWN_IDENTIFIER }
SELECT arrayMap(plus(5, _3x), [1]); -- { serverError UNKNOWN_IDENTIFIER }
SELECT arrayMap(plus(_1, _2), [1, 2, 3]); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
