-- Tags: no-parallel
-- ^ creates a user defined function, which is global.

-- The composition and the placeholders are resolved only in the analyzer.
SET enable_analyzer = 1;

-- The operator `f | g` and an ordinary call to a function named `__compose` are different
-- expressions, so they must not be considered the same expression for one alias.
DROP FUNCTION IF EXISTS __compose;
CREATE FUNCTION __compose AS (x, y) -> x + y;

SELECT arrayMap(__compose(_1, plus(_1, 1)), [1, 2]) AS x, arrayMap(_1 | plus(_1, 1), [1, 2]) AS x; -- { serverError MULTIPLE_EXPRESSIONS_FOR_ALIAS }
SELECT arrayMap(_1 | plus(_1, 1), [1, 2]) AS x, arrayMap(__compose(_1, plus(_1, 1)), [1, 2]) AS x; -- { serverError MULTIPLE_EXPRESSIONS_FOR_ALIAS }

-- The same expression written twice is still allowed.
SELECT arrayMap(__compose(_1, plus(_1, 1)), [1, 2]) AS x, arrayMap(__compose(_1, plus(_1, 1)), [1, 2]) AS x;
SELECT arrayMap(_1 | plus(_1, 1), [1, 2]) AS x, arrayMap(_1 | plus(_1, 1), [1, 2]) AS x;

-- The two expressions are different.
SELECT arrayMap(__compose(_1, plus(_1, 1)), [1, 2]) AS a, arrayMap(_1 | plus(_1, 1), [1, 2]) AS b;

DROP FUNCTION __compose;
