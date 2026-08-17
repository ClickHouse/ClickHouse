-- Tags: no-random-settings, no-random-merge-tree-settings
-- Regression test for sort order violation during SummingMergeTree merge when the
-- sort key is a hash expression over a column that contains Float32 values with
-- signaling NaN (SNaN) bit patterns.
--
-- Root cause: SummingSortedAlgorithm::setRow() routes non-aggregated column values
-- through a Field roundtrip (Float32 → Float64 as Field → Float32). On x86 this
-- silently converts a signaling NaN (SNaN) to a quiet NaN (QNaN), changing the
-- NaN bit pattern and thus the hash value. CheckSortedTransform then detects an
-- out-of-order merge output (exception in debug builds, silent data corruption in
-- release builds).
--
-- The bug also affects any column whose type contains Float32/Float64 at any
-- nesting level: Nullable(Float32), LowCardinality(Float32),
-- LowCardinality(Nullable(Float32)), Array(Float32), and so on. Wrapping types
-- route Float32 values through the same Float64 Field storage.

DROP TABLE IF EXISTS t_float_nan_sort;

-- gccMurmurHash of Float32 hashes the raw 4 bytes. Different NaN bit patterns produce
-- different hashes. generateRandom can produce non-canonical (signaling) NaN values.
CREATE TABLE t_float_nan_sort (c0 Int32, c1 Float32)
ENGINE = SummingMergeTree()
ORDER BY (gccMurmurHash(c1));

INSERT INTO TABLE t_float_nan_sort (c1, c0)
    SELECT c1, c0 FROM generateRandom('c1 Float32, c0 Int32', 4471575971265722, 3161, 5) LIMIT 130;

INSERT INTO TABLE t_float_nan_sort (c1, c0)
    SELECT c1, c0 FROM generateRandom('c1 Float32, c0 Int32', 14156128262908154975, 2463, 2) LIMIT 10;

-- This OPTIMIZE triggers a merge. Before the fix, debug builds crashed here with:
-- "Logical error: Sort order of blocks violated for column number ..."
OPTIMIZE TABLE t_float_nan_sort FINAL;

SELECT count() > 0 FROM t_float_nan_sort;

-- Also test with cityHash64 (same class of bug)
DROP TABLE IF EXISTS t_float_nan_sort_city;
CREATE TABLE t_float_nan_sort_city (c0 Int32, c1 Float32)
ENGINE = SummingMergeTree()
ORDER BY (cityHash64(c1));

INSERT INTO TABLE t_float_nan_sort_city (c1, c0)
    SELECT c1, c0 FROM generateRandom('c1 Float32, c0 Int32', 4471575971265722, 3161, 5) LIMIT 142;

INSERT INTO TABLE t_float_nan_sort_city (c1, c0)
    SELECT c1, c0 FROM generateRandom('c1 Float32, c0 Int32', 14156128262908154975, 2463, 2) LIMIT 453;

OPTIMIZE TABLE t_float_nan_sort_city FINAL;

SELECT count() > 0 FROM t_float_nan_sort_city;

-- Regression for fuzzer finding on PR #102791: Nullable(Float32) sort key input.
-- The previous narrow fix checked only plain Float32/Float64 columns and missed
-- Nullable wrappers; the fuzzer triggered the same CheckSortedTransform assertion
-- with ORDER BY cityHash64(c1) where c1 is Nullable(Float32).
DROP TABLE IF EXISTS t_float_nan_sort_nullable;
CREATE TABLE t_float_nan_sort_nullable (c0 Nullable(Int32), c1 Nullable(Float32))
ENGINE = SummingMergeTree()
ORDER BY (cityHash64(c1))
SETTINGS allow_nullable_key = 1;

INSERT INTO TABLE t_float_nan_sort_nullable (c1, c0)
    SELECT c1, c0 FROM generateRandom('c1 Float32, c0 Int32', 4471575971265722, 3161, 5) LIMIT 142;

INSERT INTO TABLE t_float_nan_sort_nullable (c1, c0)
    SELECT c1, c0 FROM generateRandom('c1 Float32, c0 Int32', 14156128262908154975, 2463, 2) LIMIT 453;

OPTIMIZE TABLE t_float_nan_sort_nullable FINAL;

SELECT count() > 0 FROM t_float_nan_sort_nullable;

DROP TABLE t_float_nan_sort;
DROP TABLE t_float_nan_sort_city;
DROP TABLE t_float_nan_sort_nullable;
