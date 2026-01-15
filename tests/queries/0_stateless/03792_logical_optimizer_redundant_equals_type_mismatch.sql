-- Test for bug fix: Logical error in LogicalExpressionOptimizerPass with type mismatch
-- The optimizer tryOptimizeOutRedundantEquals was failing when the inner boolean function
-- returned a type different from UInt8 (e.g., Nothing type when using asterisk in equals)

-- Original reproducer from the bug report
SELECT equals(equals(assumeNotNull(1234567891011), 3), equals(isNotDistinctFrom(toUInt128(3), 1234567891011), *)) AS alias856 GROUP BY GROUPING SETS ((1));

-- Additional test cases for the redundant equals optimization

-- Basic cases that should still be optimized
SELECT (1 = 1) = 1;
SELECT (1 = 2) = 0;
SELECT (1 < 2) = 1;
SELECT (isNull(NULL) = 1);
SELECT (isNotNull(1) = 1);
SELECT (empty('') = 1);

-- LowCardinality handling
SELECT (toLowCardinality('a') = 'a') = 1;
SELECT (toLowCardinality('a') = toLowCardinality('a')) = 1;

-- Nullable tuple comparison (should return NULL, not crash)
SELECT ((1, NULL) = (1, NULL)) = 1;

-- Nested equals with different result types
SELECT equals(equals(1, 1), equals(2, 2));
SELECT equals(equals(1, 2), equals(2, 1));
