-- Tags: no-random-settings, no-random-merge-tree-settings

-- Regression test for "Port is already connected" logical error in
-- `LazyUnorderedReadFromMergeTreeSource::expandPipeline` (STID 0996-3f3c).
--
-- When the lazy FINAL optimization drove the expand path, `buildReaders()`
-- returned the full list of processors produced by `ReadFromMergeTree::initializePipeline`
-- (including internal transforms whose outputs were already wired internally,
-- e.g. a `Resize`). `expandPipeline` then iterated every processor and called
-- `connect` on its first output, tripping over those already-connected ports.
--
-- The fix saves the pipe's terminal output ports before detaching processors,
-- so only real outputs are wired to the source's new inputs.
--
-- Reproduced on amd_debug with `max_threads` varying from 1 to 32.

DROP TABLE IF EXISTS t_lazy_log;

CREATE TABLE t_lazy_log (key UInt64, version UInt64, category UInt8, payload String)
ENGINE = ReplacingMergeTree(version) ORDER BY key SETTINGS index_granularity = 64;

SYSTEM STOP MERGES t_lazy_log;

INSERT INTO t_lazy_log SELECT number, 1, if(number < 50, 1, 0), 'x' FROM numbers(1000);
INSERT INTO t_lazy_log SELECT number + 1000, 1, 0, 'x' FROM numbers(1000);
INSERT INTO t_lazy_log SELECT number + 2000, 1, 0, 'x' FROM numbers(1000);
INSERT INTO t_lazy_log SELECT number * 10, 2, 2, 'x' FROM numbers(300);

-- Exercise the lazy FINAL expand path for a range of max_threads values.
-- Without the fix, one of these runs would hit the `Port is already connected`
-- exception in `LazyUnorderedReadFromMergeTreeSource::expandPipeline`.
SELECT count() FROM t_lazy_log FINAL WHERE category = 1
SETTINGS query_plan_optimize_lazy_final = 1, max_rows_for_lazy_final = 10000000, min_filtered_ratio_for_lazy_final = 0.5, enable_analyzer = 1, max_threads = 1;

SELECT count() FROM t_lazy_log FINAL WHERE category = 1
SETTINGS query_plan_optimize_lazy_final = 1, max_rows_for_lazy_final = 10000000, min_filtered_ratio_for_lazy_final = 0.5, enable_analyzer = 1, max_threads = 2;

SELECT count() FROM t_lazy_log FINAL WHERE category = 1
SETTINGS query_plan_optimize_lazy_final = 1, max_rows_for_lazy_final = 10000000, min_filtered_ratio_for_lazy_final = 0.5, enable_analyzer = 1, max_threads = 4;

SELECT count() FROM t_lazy_log FINAL WHERE category = 1
SETTINGS query_plan_optimize_lazy_final = 1, max_rows_for_lazy_final = 10000000, min_filtered_ratio_for_lazy_final = 0.5, enable_analyzer = 1, max_threads = 8;

SELECT count() FROM t_lazy_log FINAL WHERE category = 1
SETTINGS query_plan_optimize_lazy_final = 1, max_rows_for_lazy_final = 10000000, min_filtered_ratio_for_lazy_final = 0.5, enable_analyzer = 1, max_threads = 16;

SELECT count() FROM t_lazy_log FINAL WHERE category = 1
SETTINGS query_plan_optimize_lazy_final = 1, max_rows_for_lazy_final = 10000000, min_filtered_ratio_for_lazy_final = 0.5, enable_analyzer = 1, max_threads = 32;

DROP TABLE t_lazy_log;
