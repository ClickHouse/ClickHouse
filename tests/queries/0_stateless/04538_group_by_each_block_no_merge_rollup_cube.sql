-- `group_by_each_block_no_merge` flushes a partial aggregation result per block without merging
-- across blocks, so the plain `GROUP BY` output is plan-dependent. `WITH ROLLUP` / `WITH CUBE`, however,
-- run a `RollupTransform` / `CubeTransform` that accumulates every block and re-merges all of the partial
-- aggregate states before emitting, so those queries stay fully merged and deterministic regardless of the
-- setting. This pins that documented behavior: the result with the setting on must equal the result with it off.

-- WITH ROLLUP: a small `max_block_size` forces many per-block flushes on the setting-on side.
SELECT
    (SELECT groupArray((k, c)) FROM (
        SELECT number % 10 AS k, count() AS c FROM numbers(1000000)
        GROUP BY k WITH ROLLUP ORDER BY k, c
        SETTINGS group_by_each_block_no_merge = 1, max_block_size = 10000))
    =
    (SELECT groupArray((k, c)) FROM (
        SELECT number % 10 AS k, count() AS c FROM numbers(1000000)
        GROUP BY k WITH ROLLUP ORDER BY k, c
        SETTINGS group_by_each_block_no_merge = 0));

-- WITH CUBE over two keys.
SELECT
    (SELECT groupArray((a, b, c)) FROM (
        SELECT number % 5 AS a, number % 3 AS b, count() AS c FROM numbers(1000000)
        GROUP BY a, b WITH CUBE ORDER BY a, b, c
        SETTINGS group_by_each_block_no_merge = 1, max_block_size = 10000))
    =
    (SELECT groupArray((a, b, c)) FROM (
        SELECT number % 5 AS a, number % 3 AS b, count() AS c FROM numbers(1000000)
        GROUP BY a, b WITH CUBE ORDER BY a, b, c
        SETTINGS group_by_each_block_no_merge = 0));
