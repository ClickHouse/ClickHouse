-- Tags: no-random-settings, no-random-merge-tree-settings
-- no-random-*: the test pins merge/timeout settings that the randomizer would otherwise override.

-- Regression test: OPTIMIZE ... DRY RUN executes the merge synchronously inside the user
-- query, so the merge pipeline inherits the query's max_execution_time. When the soft timeout
-- fires (timeout_overflow_mode = 'break'), the merge read is truncated. The merge stage used to
-- mistake this for a normal end-of-stream, finalize a partial part, and trip the row-count
-- invariants in VerticalMergeStage with a LOGICAL_ERROR (server abort in debug/sanitizer builds).
-- A truncated DRY RUN merge must instead return TIMEOUT_EXCEEDED and keep the server alive.

DROP TABLE IF EXISTS t_dry_run_timeout;

CREATE TABLE t_dry_run_timeout (key UInt64, v1 String, v2 String)
ENGINE = MergeTree ORDER BY key
SETTINGS
    vertical_merge_algorithm_min_rows_to_activate = 1,
    vertical_merge_algorithm_min_columns_to_activate = 1,
    min_bytes_for_wide_part = 0,
    merge_max_block_size = 1;

SYSTEM STOP MERGES t_dry_run_timeout;

INSERT INTO t_dry_run_timeout SELECT number, repeat('a', 5), repeat('b', 5) FROM numbers(100000);
INSERT INTO t_dry_run_timeout SELECT number + 100000, repeat('a', 5), repeat('b', 5) FROM numbers(100000);

-- merge_max_block_size = 1 forces one merge step per row, so a 0.001s soft timeout reliably fires
-- mid-merge regardless of machine speed. Must surface as TIMEOUT_EXCEEDED, never LOGICAL_ERROR.
OPTIMIZE TABLE t_dry_run_timeout DRY RUN PARTS 'all_1_1_0', 'all_2_2_0'
SETTINGS max_execution_time = 0.001, timeout_overflow_mode = 'break'; -- { serverError TIMEOUT_EXCEEDED }

-- The server must still be alive and the parts unchanged (DRY RUN never commits a merge).
SELECT 'server alive', count() FROM t_dry_run_timeout;
SELECT name, rows FROM system.parts
WHERE database = currentDatabase() AND table = 't_dry_run_timeout' AND active
ORDER BY name;

DROP TABLE t_dry_run_timeout;
