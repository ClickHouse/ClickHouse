-- Regression test: v2 lightweight-update pipeline must not assert sort-key
-- monotonicity on its parallel-pool read output.
--
-- Before the fix, `MutationsInterpreter::execute()` unconditionally added a
-- `CheckSortedTransform` over the pipeline whenever the storage was a
-- `MergeTree`. For v1 patches that was harmless (the patch pipeline reads
-- only virtual columns so the sort-key column stayed out of the header and
-- no check was installed). For v2 patches we must read the physical source
-- columns of the sort-key expression so they land on every patch row, which
-- put the sort-key column back into the pipeline header and armed the
-- check. Lightweight updates flow through `storage->read(...)`, which
-- dispatches to `readFromPool` when `max_threads > 1`: each stream receives
-- mark ranges dynamically and is therefore NOT sort-key-monotonic per port.
-- The assertion tripped with `LOGICAL_ERROR: Sort order of blocks violated
-- for column number 0, ...`. The patch-part writer sorts the output itself,
-- so the check was also semantically unnecessary.

DROP TABLE IF EXISTS t_v2_parallel_sort_check;

CREATE TABLE t_v2_parallel_sort_check (a UInt64)
ENGINE = MergeTree ORDER BY a
SETTINGS enable_block_number_column = 1,
         enable_block_offset_column = 1,
         enable_v2_lightweight_update_patches = 1,
         apply_patches_on_merge = 0,
         index_granularity = 1024;

-- Enough rows * small granules so the read is split across parallel streams
-- (so individual streams receive non-monotonic granule ranges from the pool).
INSERT INTO t_v2_parallel_sort_check SELECT number FROM numbers(500000);

SET lightweight_delete_mode = 'lightweight_update_force';
SET max_threads = 8;

DELETE FROM t_v2_parallel_sort_check WHERE (a % 2) = 0;

SELECT count() FROM t_v2_parallel_sort_check;
SELECT count() FROM t_v2_parallel_sort_check WHERE (a % 2) = 0;
SELECT min(a), max(a) FROM t_v2_parallel_sort_check;

DROP TABLE t_v2_parallel_sort_check;
