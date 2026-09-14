-- The null-safe comparison operators are comparisons too, so a numeric literal on one side is
-- parsed from its original text against the other side's type instead of going through Float64.
--
-- `1.123456789012345728` is the Decimal the literal `1.123456789012345679` rounds to through
-- Float64, so a rounded comparison reports a match where the exact one reports none.

SET enable_analyzer = 1;

SELECT 'analyzer' AS t;
SELECT CAST('1.123456789012345728', 'Decimal128(18)') IS NOT DISTINCT FROM 1.123456789012345679 AS not_distinct;
SELECT CAST('1.123456789012345728', 'Decimal128(18)') IS DISTINCT FROM 1.123456789012345679 AS distinct_from;
SELECT isNotDistinctFrom(CAST('1.123456789012345728', 'Decimal128(18)'), 1.123456789012345679) AS not_distinct_function;
SELECT isDistinctFrom(CAST('1.123456789012345728', 'Decimal128(18)'), 1.123456789012345679) AS distinct_function;
SELECT (CAST('1.123456789012345728', 'Decimal128(18)'), 1) IS NOT DISTINCT FROM (1.123456789012345679, 1) AS tuple_not_distinct;
SELECT [CAST('1.123456789012345728', 'Decimal128(18)')] IS NOT DISTINCT FROM [1.123456789012345679] AS array_not_distinct;

-- The literal still matches the value it was written from.
SELECT CAST('1.5', 'Decimal128(18)') IS NOT DISTINCT FROM 1.5 AS scalar_hit;
SELECT (CAST('1.5', 'Decimal128(18)'), 1) IS NOT DISTINCT FROM (1.5, 1) AS tuple_hit;
SELECT [CAST('1.5', 'Decimal128(18)')] IS NOT DISTINCT FROM [1.5] AS array_hit;

-- NULL handling is unchanged.
SELECT CAST(NULL, 'Nullable(Decimal128(18))') IS NOT DISTINCT FROM 1.123456789012345679 AS null_vs_literal;
SELECT CAST(NULL, 'Nullable(Decimal128(18))') IS DISTINCT FROM 1.123456789012345679 AS null_vs_literal_distinct;

SET enable_analyzer = 0;

SELECT 'old analyzer' AS t;
SELECT CAST('1.123456789012345728', 'Decimal128(18)') IS NOT DISTINCT FROM 1.123456789012345679 AS not_distinct;
SELECT CAST('1.123456789012345728', 'Decimal128(18)') IS DISTINCT FROM 1.123456789012345679 AS distinct_from;
SELECT isNotDistinctFrom(CAST('1.123456789012345728', 'Decimal128(18)'), 1.123456789012345679) AS not_distinct_function;
SELECT (CAST('1.123456789012345728', 'Decimal128(18)'), 1) IS NOT DISTINCT FROM (1.123456789012345679, 1) AS tuple_not_distinct;
SELECT [CAST('1.123456789012345728', 'Decimal128(18)')] IS NOT DISTINCT FROM [1.123456789012345679] AS array_not_distinct;
SELECT CAST('1.5', 'Decimal128(18)') IS NOT DISTINCT FROM 1.5 AS scalar_hit;
SELECT (CAST('1.5', 'Decimal128(18)'), 1) IS NOT DISTINCT FROM (1.5, 1) AS tuple_hit;
SELECT [CAST('1.5', 'Decimal128(18)')] IS NOT DISTINCT FROM [1.5] AS array_hit;
SELECT CAST(NULL, 'Nullable(Decimal128(18))') IS NOT DISTINCT FROM 1.123456789012345679 AS null_vs_literal;
