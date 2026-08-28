-- A bracket-array right-hand side of `IN` is the set itself, so its literals have to be retargeted
-- against the left-hand side like the elements of a parenthesised list. `1.123456789012345728` is
-- the Decimal the literal `1.123456789012345679` rounds to through Float64.

SET enable_analyzer = 1;

SELECT 'analyzer' AS t;
SELECT CAST('1.123456789012345728', 'Decimal128(18)') IN [1.123456789012345679] AS bracket_array;
SELECT CAST('1.123456789012345728', 'Decimal128(18)') IN [1.123456789012345679, 9] AS bracket_array_list;
SELECT CAST('1.123456789012345728', 'Decimal128(18)') GLOBAL IN [1.123456789012345679] AS bracket_array_global;
SELECT CAST('1.5', 'Decimal128(18)') IN [1.5] AS bracket_array_hit;
SELECT CAST('1.5', 'Decimal128(18)') IN [1.5, 9] AS bracket_array_list_hit;
SELECT CAST('2.25', 'Decimal128(18)') IN [1.1, 2.25] AS bracket_array_scales_hit;
SELECT 1 IN [1, 2], 3 IN [1, 2], 1.5 IN [1.5];

SET enable_analyzer = 0;

SELECT 'old analyzer' AS t;
SELECT CAST('1.123456789012345728', 'Decimal128(18)') IN [1.123456789012345679] AS bracket_array;
SELECT CAST('1.123456789012345728', 'Decimal128(18)') IN [1.123456789012345679, 9] AS bracket_array_list;
SELECT CAST('1.123456789012345728', 'Decimal128(18)') GLOBAL IN [1.123456789012345679] AS bracket_array_global;
SELECT CAST('1.5', 'Decimal128(18)') IN [1.5] AS bracket_array_hit;
SELECT CAST('1.5', 'Decimal128(18)') IN [1.5, 9] AS bracket_array_list_hit;
SELECT CAST('2.25', 'Decimal128(18)') IN [1.1, 2.25] AS bracket_array_scales_hit;
SELECT 1 IN [1, 2], 3 IN [1, 2], 1.5 IN [1.5];
