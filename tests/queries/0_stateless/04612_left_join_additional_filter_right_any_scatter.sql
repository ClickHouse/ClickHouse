-- A LEFT JOIN whose right table has unique keys is promoted to RightAny. With a non-equi ON
-- condition the join processes only a prefix of the left block once max_joined_block_size_rows
-- candidate rows are collected, so the block is scattered to that prefix while the negated
-- null map `filter` kept the full block size. Applying that stale filter to the shorter left
-- key column raised a size-mismatch LOGICAL_ERROR.
-- Here `k = 0` fails `k > b` and `k = 1` matches.

SET enable_analyzer = 1;

-- Raised "Null map of size 2 does not match ColumnNullable of size 1".
SELECT l.k, r.a
FROM (SELECT number AS k FROM numbers(2)) AS l
LEFT JOIN (SELECT toNullable(number) AS a, 0 AS b FROM numbers(2)) AS r
  ON (l.k = r.a) AND (l.k > r.b)
ORDER BY l.k
SETTINGS join_algorithm = 'hash', join_use_nulls = 1, max_joined_block_size_rows = 1;

-- Same shape with a non-nullable right key hit the sibling "Invalid number of rows in Chunk".
SELECT l.k, r.a
FROM (SELECT number AS k FROM numbers(2)) AS l
LEFT JOIN (SELECT number AS a, 0 AS b FROM numbers(2)) AS r
  ON (l.k = r.a) AND (l.k > r.b)
ORDER BY l.k
SETTINGS join_algorithm = 'hash', join_use_nulls = 0, max_joined_block_size_rows = 1;
