-- Test: exercises `arrayFold` with non-const `Array(LowCardinality(T))` from a table.
-- Covers: src/Functions/array/arrayFold.cpp:113-122 — the non-const path in `executeImpl`.
-- The const path at line 120 calls `recursiveRemoveLowCardinality(...->convertToFullColumn())`,
-- but the non-const path keeps the LC column data and only strips the LC from the type at line 141.
-- This LC data scattering / slicing path with non-const arrays is not exercised by any existing
-- test (PR 60022 only tests const `Array(LowCardinality(T))` and existing tests use no LC arrays).

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

DROP TABLE t_array_fold_lc;
