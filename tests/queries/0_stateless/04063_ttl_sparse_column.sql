-- Regression test: `Unexpected type of result TTL column` exception when the TTL
-- expression result is a ColumnSparse.
--
-- When a table has a row TTL that directly references a column (result_column = "dt"),
-- executeExpressionAndGetColumn returns the ColumnSparse from the block without unwrapping.
-- This caused both:
--   TTLDeleteFilterTransform::extractTimestamps  (vertical merge + optimize_ttl_delete=1)
--   ITTLAlgorithm::getTimestampByIndex           (via TTLTransform in all merge paths)
-- to crash with "Unexpected type of result TTL column".
--
-- Key conditions:
--  1. Table-level row TTL = direct column reference -> result_column = "dt" is in block.
--  2. dt stored as ColumnSparse (ratio_of_defaults_for_sparse_serialization = 0.0, all dt=0).
--  3. table_ttl.min == 0 -> checkAllTTLCalculated returns false -> force_ttl = true,
--     so the TTL transform executes even though no row actually has an expired TTL.
--  4. Vertical merge is activated (thresholds set low) for test 1.
--     Horizontal merge is forced (thresholds set high) for test 2.

-- Test 1: TTLDeleteFilterTransform::extractTimestamps (vertical merge path).
DROP TABLE IF EXISTS t_ttl_sparse_v;

CREATE TABLE t_ttl_sparse_v
(
    id  UInt64,
    dt  DateTime
)
ENGINE = MergeTree
ORDER BY id
TTL dt  -- result_column = "dt"; block.has("dt") = true -> ColumnSparse returned directly
SETTINGS
    ratio_of_defaults_for_sparse_serialization = 0.0,
    vertical_merge_algorithm_min_rows_to_activate = 0,
    vertical_merge_algorithm_min_columns_to_activate = 1,
    vertical_merge_optimize_ttl_delete = 1,
    merge_with_ttl_timeout = 0;

SYSTEM STOP MERGES t_ttl_sparse_v;

-- dt = toDateTime(0) is the DateTime default value (100% defaults with ratio=0.0
-- means column IS sparse). table_ttl.min = 0 -> force_ttl = true on merge.
INSERT INTO t_ttl_sparse_v SELECT number, toDateTime(0) FROM numbers(10);
INSERT INTO t_ttl_sparse_v SELECT number + 10, toDateTime(0) FROM numbers(10);
INSERT INTO t_ttl_sparse_v SELECT number + 20, toDateTime(0) FROM numbers(10);
INSERT INTO t_ttl_sparse_v SELECT number + 30, toDateTime(0) FROM numbers(10);
INSERT INTO t_ttl_sparse_v SELECT number + 40, toDateTime(0) FROM numbers(10);

SYSTEM START MERGES t_ttl_sparse_v;
OPTIMIZE TABLE t_ttl_sparse_v FINAL;

-- isTTLExpired(0, ...) = false -> no rows deleted (TTL=0 is the "never expires" sentinel).
-- Before fix: server crashed in extractTimestamps with "Unexpected type of result TTL column".
-- After fix: ColumnSparse is converted to dense; all timestamps = 0 (not expired); 50 rows survive.
SELECT count() FROM t_ttl_sparse_v;

DROP TABLE t_ttl_sparse_v;


-- Test 2: ITTLAlgorithm::getTimestampByIndex via TTLTransform (horizontal merge path).
-- Use large vertical-merge thresholds to force horizontal merge, so dt is in the merged
-- block and getTimestampByIndex is called directly on the sparse ColumnUInt32.
DROP TABLE IF EXISTS t_ttl_sparse_h;

CREATE TABLE t_ttl_sparse_h
(
    id  UInt64,
    dt  DateTime
)
ENGINE = MergeTree
ORDER BY id
TTL dt
SETTINGS
    ratio_of_defaults_for_sparse_serialization = 0.0,
    vertical_merge_algorithm_min_rows_to_activate = 1000000,
    vertical_merge_algorithm_min_columns_to_activate = 100,
    merge_with_ttl_timeout = 0;

SYSTEM STOP MERGES t_ttl_sparse_h;

INSERT INTO t_ttl_sparse_h SELECT number, toDateTime(0) FROM numbers(10);
INSERT INTO t_ttl_sparse_h SELECT number + 10, toDateTime(0) FROM numbers(10);
INSERT INTO t_ttl_sparse_h SELECT number + 20, toDateTime(0) FROM numbers(10);
INSERT INTO t_ttl_sparse_h SELECT number + 30, toDateTime(0) FROM numbers(10);
INSERT INTO t_ttl_sparse_h SELECT number + 40, toDateTime(0) FROM numbers(10);

SYSTEM START MERGES t_ttl_sparse_h;
OPTIMIZE TABLE t_ttl_sparse_h FINAL;

-- Before fix: server crashed in getTimestampByIndex with sparse ColumnUInt32.
-- After fix: ColumnSparse is delegated to getValuesColumn+getValueIndex; all timestamps=0;
--            isTTLExpired(0,...)=false; 50 rows survive.
SELECT count() FROM t_ttl_sparse_h;

DROP TABLE t_ttl_sparse_h;
