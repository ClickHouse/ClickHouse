-- A column read from MergeTree with sparse serialization must not end up as the data column of a
-- `ColumnConst` produced by `optimize_constant_columns_after_filter`: a nested `ColumnSparse` is
-- invisible to `IColumn::isSparse`, so functions and `Aggregator` would read raw data that is not there.

DROP TABLE IF EXISTS t_filter_const_sparse;

CREATE TABLE t_filter_const_sparse (a UInt64, b Int32)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS ratio_of_defaults_for_sparse_serialization = 0.0, min_bytes_for_wide_part = 0;

INSERT INTO t_filter_const_sparse SELECT 0, 0 FROM numbers(10000);

SELECT column, serialization_kind FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_filter_const_sparse' AND active
ORDER BY column;

SET optimize_constant_columns_after_filter = 1;

-- A function over the constant column: used to throw `LOGICAL_ERROR` from `FunctionBinaryArithmetic`.
SELECT DISTINCT a % 7 FROM t_filter_const_sparse WHERE a = 0;

-- Aggregation in order over the constant key: used to throw `Bad cast from type DB::ColumnSparse`.
SELECT b, count() FROM t_filter_const_sparse WHERE b = 0 GROUP BY b
SETTINGS optimize_aggregation_in_order = 1;

SELECT a, count() FROM t_filter_const_sparse WHERE a = 0 GROUP BY a
SETTINGS optimize_aggregation_in_order = 1;

-- Two-level conversion and merging of partially aggregated blocks with a constant key.
SELECT a, b, count() FROM t_filter_const_sparse WHERE a = 0 AND b = 0 GROUP BY a, b
SETTINGS group_by_two_level_threshold = 1, group_by_two_level_threshold_bytes = 1, max_threads = 4;

-- The same values must be produced with the optimization turned off.
SELECT DISTINCT a % 7 FROM t_filter_const_sparse WHERE a = 0 SETTINGS optimize_constant_columns_after_filter = 0;

DROP TABLE t_filter_const_sparse;
