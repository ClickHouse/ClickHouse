-- A LEFT JOIN whose right table has unique keys is promoted to RightAny. With a non-equi ON
-- condition the join processes only a prefix of the left block once max_joined_block_size_rows
-- candidate rows are collected, so the block is scattered to that prefix while the negated
-- null map `filter` kept the full block size. Applying that stale filter to the shorter left
-- key column raised a size-mismatch LOGICAL_ERROR.
-- Here `k = 0` fails `k > b` and `k = 1` matches.

SET enable_analyzer = 1;
-- Swapping the tables turns this into a RIGHT join, which is not promoted to RightAny and takes
-- the need_replication path that always resized `filter`, so neither query would reproduce.
SET query_plan_join_swap_table = false;

-- Raised "Null map of size 2 at offset 0 does not match ColumnNullable of size 1".
SELECT l.k, r.a
FROM (SELECT number AS k FROM numbers(2)) AS l
LEFT JOIN (SELECT toNullable(number) AS a, 0 AS b FROM numbers(2)) AS r
  ON (l.k = r.a) AND (l.k > r.b)
ORDER BY l.k
SETTINGS join_algorithm = 'hash', join_use_nulls = 1, max_joined_block_size_rows = 1,
    log_comment = '04612_split';

-- Same shape with a non-nullable right key hit the sibling "Invalid number of rows in Chunk".
SELECT l.k, r.a
FROM (SELECT number AS k FROM numbers(2)) AS l
LEFT JOIN (SELECT number AS a, 0 AS b FROM numbers(2)) AS r
  ON (l.k = r.a) AND (l.k > r.b)
ORDER BY l.k
SETTINGS join_algorithm = 'hash', join_use_nulls = 0, max_joined_block_size_rows = 1;

-- The results above are identical with and without the split, so assert the split happened:
-- the deferred left row is probed again in the following block, giving 3 probed rows for 2
-- left rows. Without it the queries no longer reach the code path they cover.
SYSTEM FLUSH LOGS query_log;
SELECT if(probed = 3, 'ok', format('fail: {}', probed))
FROM (
    SELECT argMax(ProfileEvents['JoinProbeTableRowCount'], event_time_microseconds) AS probed
    FROM system.query_log
    WHERE type = 'QueryFinish' AND current_database = currentDatabase()
      AND Settings['log_comment'] = '04612_split'
);
