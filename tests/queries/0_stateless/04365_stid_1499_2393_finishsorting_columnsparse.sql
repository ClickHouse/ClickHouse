-- Tags: no-random-merge-tree-settings
-- ^ the repro needs a sparse-serialized sort-key part next to a dense one; randomized
--   ratio_of_defaults_for_sparse_serialization would defeat that.

-- STID 1499-2393: `Bad cast from type DB::ColumnSparse to DB::ColumnVector<unsigned short>`
-- in FinishSortingTransform::consume -> less -> ColumnVector<UInt16>::doCompareAt.
-- Captured by the AST fuzzer on 02149_read_in_order_fixed_prefix over amd_msan. The read-in-order
-- sort prefix (toStartOfMonth(date), a Date/UInt16) reached the cross-chunk compare as a dense
-- column in one chunk and a ColumnSparse in the next. The query must complete without the bad cast.

DROP TABLE IF EXISTS t_stid_1499_2393;

CREATE TABLE t_stid_1499_2393(date Date, i UInt64, v UInt64)
ENGINE = MergeTree ORDER BY (date, i)
SETTINGS index_granularity = 8192, ratio_of_defaults_for_sparse_serialization = 0.5, min_bytes_for_wide_part = 0;

SYSTEM STOP MERGES t_stid_1499_2393;

-- A part whose `date` is entirely the default (1970-01-01 == 0) serializes `date` as ColumnSparse.
INSERT INTO t_stid_1499_2393 SELECT toDate(0), number % 10, number FROM numbers(100000);
-- Parts with a non-default `date` serialize `date` as a dense column.
INSERT INTO t_stid_1499_2393 SELECT '2020-10-10', number % 10, number FROM numbers(100000);
INSERT INTO t_stid_1499_2393 SELECT '2020-10-12', number, number FROM numbers(100000);

-- The exact fuzzed query shape: read-in-order sort by the derived prefix, direction mismatch
-- (`d DESC, -i`) so FinishSortingTransform is used. Output ordering is irrelevant.
SELECT toStartOfMonth(date) AS d, i FROM t_stid_1499_2393
ORDER BY d DESC, -i LIMIT 5
SETTINGS optimize_read_in_order = 1, read_in_order_use_virtual_row = 1, max_threads = 8
FORMAT Null;

SELECT toStartOfMonth(date) AS d, i FROM t_stid_1499_2393
ORDER BY d, -i LIMIT 5
SETTINGS optimize_read_in_order = 1, read_in_order_use_virtual_row = 1, max_threads = 8
FORMAT Null;

DROP TABLE t_stid_1499_2393;
