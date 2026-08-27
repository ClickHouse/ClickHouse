-- A numeric literal keeps its original text so a comparison against a Decimal stays exact. That has
-- to hold when the literal sits inside a tuple or an array too, both for a direct comparison and on
-- the right of IN, and not only when it stands on its own.
--
-- `1.123456789012345728` is the Decimal the literal `1.123456789012345679` rounds to through
-- Float64, so a rounded comparison reports a match where the exact one reports none.

SET enable_analyzer = 1;

SELECT 'analyzer' AS t;
SELECT CAST('1.123456789012345728', 'Decimal128(18)') = 1.123456789012345679 AS scalar_equals;
SELECT CAST('1.123456789012345728', 'Decimal128(18)') IN (1.123456789012345679) AS scalar_in;
SELECT CAST('1.123456789012345728', 'Decimal128(18)') IN (1.123456789012345679, 9) AS scalar_in_list;
SELECT (CAST('1.123456789012345728', 'Decimal128(18)'), 1) = (1.123456789012345679, 1) AS tuple_equals;
SELECT (CAST('1.123456789012345728', 'Decimal128(18)'), 1) IN ((1.123456789012345679, 1)) AS tuple_in;
SELECT (CAST('1.123456789012345728', 'Decimal128(18)'), 1) IN ((1.123456789012345679, 1), (9, 9)) AS tuple_in_list;
SELECT [CAST('1.123456789012345728', 'Decimal128(18)')] = [1.123456789012345679] AS array_equals;
SELECT [CAST('1.123456789012345728', 'Decimal128(18)')] IN ([1.123456789012345679]) AS array_in;
SELECT [CAST('1.123456789012345728', 'Decimal128(18)')] IN ([1.123456789012345679], [9]) AS array_in_list;

-- The literal still matches the value it was written from, in every carrier.
SELECT CAST('1.5', 'Decimal128(18)') IN (1.5) AS scalar_hit;
SELECT (CAST('1.5', 'Decimal128(18)'), 1) = (1.5, 1) AS tuple_equals_hit;
SELECT (CAST('1.5', 'Decimal128(18)'), 1) IN ((1.5, 1)) AS tuple_in_hit;
SELECT (CAST('1.5', 'Decimal128(18)'), 1) IN ((1.5, 1), (2, 2)) AS tuple_in_list_hit;
SELECT [CAST('1.5', 'Decimal128(18)')] = [1.5] AS array_equals_hit;
SELECT [CAST('1.5', 'Decimal128(18)')] IN ([1.5]) AS array_in_hit;
SELECT [CAST('1.5', 'Decimal128(18)')] IN ([1.5], [2]) AS array_in_list_hit;

-- Plain literal-only comparisons are untouched.
SELECT (1, 2) IN ((1, 2)), [1] IN ([1]), (1, 2) IN ((3, 4)), [1] IN ([2]), (1.5, 2) = (1.5, 2), [1.5, 2.5] = [1.5, 2.5];

SET enable_analyzer = 0;

SELECT 'old analyzer' AS t;
SELECT CAST('1.123456789012345728', 'Decimal128(18)') = 1.123456789012345679 AS scalar_equals;
SELECT (CAST('1.123456789012345728', 'Decimal128(18)'), 1) = (1.123456789012345679, 1) AS tuple_equals;
SELECT (CAST('1.123456789012345728', 'Decimal128(18)'), 1) IN ((1.123456789012345679, 1)) AS tuple_in;
SELECT (CAST('1.123456789012345728', 'Decimal128(18)'), 1) IN ((1.123456789012345679, 1), (9, 9)) AS tuple_in_list;
SELECT [CAST('1.123456789012345728', 'Decimal128(18)')] = [1.123456789012345679] AS array_equals;
SELECT [CAST('1.123456789012345728', 'Decimal128(18)')] IN ([1.123456789012345679]) AS array_in;
SELECT [CAST('1.123456789012345728', 'Decimal128(18)')] IN ([1.123456789012345679], [9]) AS array_in_list;
SELECT (CAST('1.5', 'Decimal128(18)'), 1) IN ((1.5, 1)) AS tuple_in_hit;
SELECT [CAST('1.5', 'Decimal128(18)')] IN ([1.5], [2]) AS array_in_list_hit;
SELECT (1, 2) IN ((1, 2)), [1] IN ([1]), (1, 2) IN ((3, 4)), [1] IN ([2]), (1.5, 2) = (1.5, 2), [1.5, 2.5] = [1.5, 2.5];
