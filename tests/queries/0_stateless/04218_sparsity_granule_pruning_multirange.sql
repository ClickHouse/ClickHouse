-- Regression test for a bug in `SparseGranuleAnalyzer::analyzeSparseColumnGranules`:
-- when the analyzer is handed non-contiguous `MarkRanges` (e.g. PK / skip-index /
-- query-condition-cache trimmed some granules out), a stale `continue_reading=true`
-- carried across ranges made the merge tree reader skip the seek between ranges,
-- and offsets were read from the wrong position. Fix: reset `continue_reading=false`
-- at the start of each range.
--
-- We use a private per-test table so the query condition cache (server-wide, keyed
-- on `(table_uuid, ...)`) can't be cleared by a parallel test, but also can't
-- contaminate other tests. No `SYSTEM DROP QUERY CONDITION CACHE` is needed.

DROP TABLE IF EXISTS t_phaseB_multirange;

CREATE TABLE t_phaseB_multirange
(
    id UInt64,
    x UInt32
)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 100, ratio_of_defaults_for_sparse_serialization = 0.5;

SYSTEM STOP MERGES t_phaseB_multirange;

-- 1000 rows. Non-defaults only at rows 0, 200, 400, 600, 800 (granules 0, 2, 4, 6, 8).
-- Granules 1, 3, 5, 7, 9 are all-default and prunable. Each non-default has x = 1
-- so `sum(x)` gives an exact count of non-defaults read by the scan.
INSERT INTO t_phaseB_multirange SELECT number, if(number % 200 = 0, 1, 0) FROM numbers(1000) SETTINGS optimize_on_insert = 0;

SELECT serialization_kind FROM system.parts_columns
WHERE table = 't_phaseB_multirange' AND database = currentDatabase() AND column = 'x';

SET optimize_trivial_count_with_sparsity_filter = 0;
SET use_sparsity_info_for_pruning = 'planning';

-- First query warms the cache: ReadFromMergeTree writes Phase B's trim diff
-- `{1, 3, 5, 7, 9} -> no matches for x != 0` into the query condition cache.
SELECT count() FROM t_phaseB_multirange WHERE x != 0;

-- Second query: the cache cuts those granules at planning time, so Phase B
-- receives `MarkRanges = [{0,1}, {2,3}, {4,5}, {6,7}, {8,9}]` -- five disjoint
-- single-mark ranges. Pre-fix the analyzer over-pruned and `sum(x)` returned 3.
SELECT count() FROM t_phaseB_multirange WHERE x != 0;
SELECT sum(x)  FROM t_phaseB_multirange WHERE x != 0;

-- Interleaved repeats to exercise the same code path multiple times.
SELECT sum(x)  FROM t_phaseB_multirange WHERE x != 0;
SELECT count() FROM t_phaseB_multirange WHERE x != 0;
SELECT sum(x)  FROM t_phaseB_multirange WHERE x != 0;

-- With Phase B turned off but the cache potentially populated by the planning-mode
-- queries above, results must still match the baseline (the cache entries Phase B
-- wrote must be correct).
SELECT sum(x)  FROM t_phaseB_multirange WHERE x != 0 SETTINGS use_sparsity_info_for_pruning = 'off';
SELECT count() FROM t_phaseB_multirange WHERE x != 0 SETTINGS use_sparsity_info_for_pruning = 'off';

DROP TABLE t_phaseB_multirange;
