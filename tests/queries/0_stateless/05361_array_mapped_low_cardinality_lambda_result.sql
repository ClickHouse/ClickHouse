-- Higher-order array functions accept lambdas returning `LowCardinality` results,
-- and the result type does not keep `LowCardinality`, because execution materializes it.

SELECT arrayFilter(x -> toLowCardinality(toUInt8(x > 1)), [1, 2, 3]) AS r, toTypeName(r);
SELECT arrayFilter(x -> toLowCardinality(toNullable(toUInt8(x > 1))), [1, 2, 3]) AS r, toTypeName(r);
SELECT arrayExists(x -> toLowCardinality(toUInt8(x > 2)), [1, 2, 3]) AS r, toTypeName(r);
SELECT arrayAll(x -> toLowCardinality(toUInt8(x > 0)), [1, 2, 3]) AS r, toTypeName(r);
SELECT arrayCount(x -> toLowCardinality(toUInt8(x > 1)), [1, 2, 3]) AS r, toTypeName(r);
SELECT arrayFirst(x -> toLowCardinality(toUInt8(x > 1)), [1, 2, 3]) AS r, toTypeName(r);
SELECT arrayFirstIndex(x -> toLowCardinality(toUInt8(x > 1)), [1, 2, 3]) AS r, toTypeName(r);
SELECT arraySplit(x -> toLowCardinality(toUInt8(x = 2)), [1, 2, 3]) AS r, toTypeName(r);
SELECT arrayFill(x -> toLowCardinality(toUInt8(x != 2)), [1, 2, 3]) AS r, toTypeName(r);
SELECT arrayCumSum(x -> toLowCardinality(toUInt8(x)), [1, 2, 3]) AS r, toTypeName(r);
SELECT arrayMap(x -> toLowCardinality(toString(x)), [1, 2, 3]) AS r, toTypeName(r);
SELECT arraySort(x -> toLowCardinality(toString(-x)), [1, 2, 3]) AS r, toTypeName(r);
