-- Test: exercises `arrayFold` with non-const `Array(LowCardinality(T))` from a table.
-- Covers the non-const path in `FunctionArrayFold::executeImpl`, which builds the nested
-- data column for the lambda. That column must have LowCardinality stripped from BOTH the
-- column and the type; stripping only the type leaves a LowCardinality column carrying a
-- non-LowCardinality type, so the lambda's binary functions hit a type/column mismatch.
-- The const path already strips both via recursiveRemoveLowCardinality(convertToFullColumn()).

SET allow_suspicious_low_cardinality_types = 1;

DROP TABLE IF EXISTS t_array_fold_lc;
CREATE TABLE t_array_fold_lc (id UInt64, a Array(LowCardinality(Int64))) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_array_fold_lc VALUES (1, [1,2,3,4]), (2, [10,20]), (3, []), (4, [100]);

-- Multi-row, non-const, varying array sizes — exercises slice/scatter on LC data.
SELECT id, arrayFold((acc, x) -> toLowCardinality(acc + x), a, toLowCardinality(toInt64(0)))
FROM t_array_fold_lc
ORDER BY id;

-- materialize() forces the column to be non-const while remaining single-row.
SELECT arrayFold((acc, x) -> toLowCardinality(acc + x), materialize([1, 2, 3]::Array(LowCardinality(Int64))), toLowCardinality(toInt64(0)));

-- Empty non-const LC array — exercises the empty-array branch on the non-const LC path.
SELECT arrayFold((acc, x) -> toLowCardinality(acc + x), materialize([]::Array(LowCardinality(Int64))), toLowCardinality(toInt64(99)));

-- Plain (non-LowCardinality) accumulator and lambda body over a non-const Array(LowCardinality(T)).
-- Here the lambda result is not LowCardinality, so the default LowCardinality function adaptor does
-- not fully unwrap arguments; the nested data column reaches `plus` still LowCardinality-wrapped while
-- its type is plain, which used to raise a LOGICAL_ERROR ("Arguments of 'plus' have incorrect data
-- types"). Regression test for that path.
SELECT id, arrayFold((acc, x) -> acc + x, a, toInt64(0))
FROM t_array_fold_lc
ORDER BY id;

SELECT arrayFold((acc, x) -> acc + x, materialize([1, 2, 3]::Array(LowCardinality(Int64))), toInt64(0));

-- Two non-const LowCardinality arrays with a plain accumulator (multi-array scatter path).
SELECT arrayFold((acc, x, y) -> acc + x + y,
    materialize([1, 2, 3]::Array(LowCardinality(Int64))),
    materialize([10, 20, 30]::Array(LowCardinality(Int64))),
    toInt64(0));

-- LowCardinality(String) elements with a plain String accumulator.
SELECT arrayFold((acc, x) -> concat(acc, x), materialize(['a', 'b', 'c']::Array(LowCardinality(String))), ''::String);

-- Empty non-const LowCardinality array with a plain accumulator.
SELECT arrayFold((acc, x) -> acc + x, materialize([]::Array(LowCardinality(Int64))), toInt64(42));

DROP TABLE t_array_fold_lc;
