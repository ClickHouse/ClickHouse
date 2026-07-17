-- The legacy read-in-order shortcut for window functions (`tryReuseStorageOrderingForWindowFunctions`) peels
-- `WindowStep <- SortingStep <- [Expression] <- ReadFromMergeTree` and pushes the outer `LIMIT` into the storage read.
-- A stateful function (e.g. `neighbor`) in that pre-window `Expression` (the old-analyzer `before_window` expression)
-- must observe every source block, but pushing the `LIMIT` stops the read after the first row, so `neighbor(v, 1)`
-- returned the default 0 instead of the next row's value. `index_granularity = 1` makes the pushed limit actually
-- truncate the read; `max_block_size` / `max_threads` are pinned so the fixed read still fits one block.
SET allow_deprecated_error_prone_window_functions = 1;

DROP TABLE IF EXISTS t_04539;
CREATE TABLE t_04539 (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 1;
INSERT INTO t_04539 VALUES (1, 10), (2, 20), (3, 30);

-- Expected: neighbor(v, 1) of the first row (k = 1) is the second row's v = 20 (not 0).
SELECT neighbor(v, 1) AS n, row_number() OVER (ORDER BY k) AS rn
FROM t_04539 ORDER BY k LIMIT 1
SETTINGS allow_experimental_analyzer = 0, query_plan_reuse_storage_ordering_for_window_functions = 1,
         optimize_read_in_order = 0, max_block_size = 1000, max_threads = 1;

DROP TABLE t_04539;
