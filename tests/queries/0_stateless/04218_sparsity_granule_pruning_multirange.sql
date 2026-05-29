-- Regression test for non-contiguous `MarkRanges` (e.g. when the PK / skip-index /
-- query-condition-cache already trimmed some granules out): the analyzer must reset
-- `continue_reading` between ranges, otherwise the reader skips the seek and reads
-- offsets from the wrong position.
--
-- The query-condition-cache is keyed by `(table_uuid, ...)`; this test uses a
-- per-test database, so entries don't leak between parallel runs.

DROP TABLE IF EXISTS t_sparse_multirange;

CREATE TABLE t_sparse_multirange
(
    id UInt64,
    x UInt32
)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 100, ratio_of_defaults_for_sparse_serialization = 0.5, compute_exact_num_defaults_for_sparse_columns = 1;

SYSTEM STOP MERGES t_sparse_multirange;

-- 1000 rows. Non-defaults only at rows 0, 200, 400, 600, 800 (granules 0, 2, 4, 6, 8).
-- Granules 1, 3, 5, 7, 9 are all-default and prunable. Each non-default has x = 1
-- so `sum(x)` gives an exact count of non-defaults read by the scan.
INSERT INTO t_sparse_multirange SELECT number, if(number % 200 = 0, 1, 0) FROM numbers(1000) SETTINGS optimize_on_insert = 0;

SELECT serialization_kind FROM system.parts_columns
WHERE table = 't_sparse_multirange' AND database = currentDatabase() AND column = 'x';

SET optimize_trivial_count_with_sparsity_filter = 0;
SET use_sparsity_info_for_pruning = 'planning';

-- First query warms the cache: `ReadFromMergeTree` writes the trim diff
-- `{1, 3, 5, 7, 9} -> no matches for x != 0` into the query condition cache.
SELECT count() FROM t_sparse_multirange WHERE x != 0;

-- Second query: the cache cuts those granules at plan time, so the analyzer now
-- receives `MarkRanges = [{0,1}, {2,3}, {4,5}, {6,7}, {8,9}]` -- five disjoint
-- single-mark ranges. Without the fix the analyzer over-prunes and `sum(x)` returns 3.
SELECT count() FROM t_sparse_multirange WHERE x != 0;
SELECT sum(x)  FROM t_sparse_multirange WHERE x != 0;

-- Interleaved repeats to exercise the same code path multiple times.
SELECT sum(x)  FROM t_sparse_multirange WHERE x != 0;
SELECT count() FROM t_sparse_multirange WHERE x != 0;
SELECT sum(x)  FROM t_sparse_multirange WHERE x != 0;

-- With pruning turned off, results must still match the baseline -- the cache
-- entries written above must be correct on their own.
SELECT sum(x)  FROM t_sparse_multirange WHERE x != 0 SETTINGS use_sparsity_info_for_pruning = 'off';
SELECT count() FROM t_sparse_multirange WHERE x != 0 SETTINGS use_sparsity_info_for_pruning = 'off';

DROP TABLE t_sparse_multirange;
