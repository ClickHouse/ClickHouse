-- With the default `optimize_move_to_prewhere = 1`, `MergeTreeWhereOptimizer` rewrites a `WHERE` into a
-- reader-side `PREWHERE`. When the `WHERE` also contains a stateful function (e.g. `neighbor`,
-- `runningAccumulate`, `logTrace`), a deterministic sibling conjunct (`key % 2 = 0`) must NOT be moved to
-- `PREWHERE`: the reader would filter rows before the stateful predicate runs, so a block- and order-dependent
-- function would observe the wrong rows. This exercises the automatic `WHERE` -> `PREWHERE` move for both the
-- planner (`optimizePrewhere`) and the old-analyzer (`try_move_to_prewhere`) paths.

DROP TABLE IF EXISTS t_04555;
CREATE TABLE t_04555 (key UInt64, v UInt64) ENGINE = MergeTree ORDER BY key;
INSERT INTO t_04555 SELECT number, number FROM numbers(100);

-- In one block of rows 0..99 sorted by key, `neighbor(v, 1) = v + 1` holds for every row except the last.
-- With `key % 2 = 0` kept in the same `WHERE`, the filter runs on the full block, so the 50 even rows match.
-- If `key % 2 = 0` were moved to `PREWHERE`, the reader would drop the odd rows first, `neighbor` would then
-- see only even values (0, 2, 4, ...), and the equality would never hold - the count would collapse to 0.
-- `max_block_size` is pinned large so all rows form a single block regardless of randomized settings.
SELECT count() FROM t_04555 WHERE key % 2 = 0 AND neighbor(v, 1) = v + 1
    SETTINGS enable_analyzer = 1, max_threads = 1, max_block_size = 65536,
        optimize_move_to_prewhere = 1, enable_parallel_replicas = 0,
        allow_deprecated_error_prone_window_functions = 1;

SELECT count() FROM t_04555 WHERE key % 2 = 0 AND neighbor(v, 1) = v + 1
    SETTINGS enable_analyzer = 0, max_threads = 1, max_block_size = 65536,
        optimize_move_to_prewhere = 1, enable_parallel_replicas = 0,
        allow_deprecated_error_prone_window_functions = 1;

DROP TABLE t_04555;
