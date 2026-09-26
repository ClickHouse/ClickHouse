-- Regression test: GROUPING SETS with group_by_use_nulls and LowCardinality nested inside Tuple
-- caused "Block structure mismatch in Pipe stream: different types" exception.
-- The toNullable function strips nested LowCardinality, but the output header for missing keys
-- preserved it, causing a type mismatch between grouping sets.

SET group_by_use_nulls = 1;
SET enable_positional_arguments = 1;
SET allow_suspicious_low_cardinality_types = 1;

SELECT tuple(toLowCardinality(1), 2) AS a GROUP BY GROUPING SETS ((1), (isNullable(19)));
