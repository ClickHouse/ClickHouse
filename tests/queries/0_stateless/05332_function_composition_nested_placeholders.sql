-- Placeholders in the lambda position of a nested higher-order function call belong to that
-- call, not to the enclosing one.

SET enable_analyzer = 1;

SELECT arrayMap(x -> arrayMap(y -> y + 1, x), [[1, 2], [3]]);
SELECT arrayMap(arrayMap(_ + 1, _1), [[1, 2], [3]]);
SELECT arrayMap(arrayMap(_ + 1, _), [[1, 2], [3]]);
SELECT arrayMap(arrayFilter(_ > 1, _1), [[1, 2], [3]]);
SELECT arrayMap(arraySum(_ * 10, _1) + 1, [[1, 2], [3]]);
SELECT arrayMap(_1 | arrayMap(_ * 2, _1), [[1, 2], [3]]);
SELECT arrayMap(arrayMap(_ + 1, _1) | arraySum, [[1, 2], [3]]);

-- A numbered placeholder in the nested lambda position is bound by the enclosing lambda, so the
-- nested call is not lifted, the same as `arrayMap(_1 -> arrayFilter(_1 > 1, _1), ...)`.
SELECT arrayMap(arrayFilter(_1 > 1, _1), [[1, 2], [3]]); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- The nested call has no placeholders of its own outside its lambda position, so the enclosing
-- expression has none either and is not lifted.
SELECT arrayMap(arrayMap(_ + 1, [1, 2]), [[1, 2], [3]]); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
