-- The exact-comparison contract must not depend on the spelling of the composite. `[1.1]` is one
-- literal, while `array(1.1)` is a function that is constant-folded, and both have to keep the
-- original literal text until the compared type is known. Same for `tuple(...)` and `map(...)`.
--
-- `1.123456789012345728` is the Decimal the literal `1.123456789012345679` rounds to through
-- Float64, so a rounded comparison reports a match where the exact one reports none.

SET enable_analyzer = 1;

SELECT 'analyzer' AS t;
SELECT [CAST('1.123456789012345728', 'Decimal128(18)')] = array(1.123456789012345679) AS array_fn_equals;
SELECT (CAST('1.123456789012345728', 'Decimal128(18)'), 1) = tuple(1.123456789012345679, 1) AS tuple_fn_equals;
SELECT [CAST('1.123456789012345728', 'Decimal128(18)')] IN (array(1.123456789012345679)) AS array_fn_in;
SELECT CAST('1.123456789012345728', 'Decimal128(18)') IN array(1.123456789012345679) AS scalar_in_array_fn;
SELECT (CAST('1.123456789012345728', 'Decimal128(18)'), 1) IN (tuple(1.123456789012345679, 1)) AS tuple_fn_in;
SELECT CAST(map('a', CAST('1.123456789012345728', 'Decimal128(18)')), 'Map(String, Decimal128(18))') IN (map('a', 1.123456789012345679)) AS map_in;

-- The literal still matches the value it was written from.
SELECT [CAST('1.5', 'Decimal128(18)')] = array(1.5) AS array_fn_hit;
SELECT (CAST('1.5', 'Decimal128(18)'), 1) = tuple(1.5, 1) AS tuple_fn_hit;
SELECT [CAST('1.5', 'Decimal128(18)')] IN (array(1.5)) AS array_fn_in_hit;
SELECT CAST('1.5', 'Decimal128(18)') IN array(1.5) AS scalar_in_array_fn_hit;
SELECT (CAST('1.5', 'Decimal128(18)'), 1) IN (tuple(1.5, 1)) AS tuple_fn_in_hit;
SELECT CAST(map('a', CAST('1.5', 'Decimal128(18)')), 'Map(String, Decimal128(18))') IN (map('a', 1.5)) AS map_in_hit;

-- Literal-only comparisons are untouched.
SELECT array(1.5) = [1.5], tuple(1.5, 2) = (1.5, 2), map('a', 1.5) = map('a', 1.5), array(1) IN ([1]), array(1) IN ([2]);

SET enable_analyzer = 0;

SELECT 'old analyzer' AS t;
SELECT [CAST('1.123456789012345728', 'Decimal128(18)')] = array(1.123456789012345679) AS array_fn_equals;
SELECT (CAST('1.123456789012345728', 'Decimal128(18)'), 1) = tuple(1.123456789012345679, 1) AS tuple_fn_equals;
SELECT [CAST('1.123456789012345728', 'Decimal128(18)')] IN (array(1.123456789012345679)) AS array_fn_in;
SELECT CAST('1.123456789012345728', 'Decimal128(18)') IN array(1.123456789012345679) AS scalar_in_array_fn;
SELECT (CAST('1.123456789012345728', 'Decimal128(18)'), 1) IN (tuple(1.123456789012345679, 1)) AS tuple_fn_in;
SELECT CAST(map('a', CAST('1.123456789012345728', 'Decimal128(18)')), 'Map(String, Decimal128(18))') IN (map('a', 1.123456789012345679)) AS map_in;
SELECT [CAST('1.5', 'Decimal128(18)')] = array(1.5) AS array_fn_hit;
SELECT (CAST('1.5', 'Decimal128(18)'), 1) = tuple(1.5, 1) AS tuple_fn_hit;
SELECT [CAST('1.5', 'Decimal128(18)')] IN (array(1.5)) AS array_fn_in_hit;
SELECT CAST('1.5', 'Decimal128(18)') IN array(1.5) AS scalar_in_array_fn_hit;
SELECT (CAST('1.5', 'Decimal128(18)'), 1) IN (tuple(1.5, 1)) AS tuple_fn_in_hit;
SELECT CAST(map('a', CAST('1.5', 'Decimal128(18)')), 'Map(String, Decimal128(18))') IN (map('a', 1.5)) AS map_in_hit;
SELECT array(1.5) = [1.5], tuple(1.5, 2) = (1.5, 2), map('a', 1.5) = map('a', 1.5), array(1) IN ([1]), array(1) IN ([2]);
