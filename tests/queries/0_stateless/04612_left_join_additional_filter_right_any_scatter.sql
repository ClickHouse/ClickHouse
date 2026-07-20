-- A LEFT JOIN with a non-equi ON condition whose right table has unique keys is promoted to
-- RightAny. The additional-filter code path can drop trailing left rows, so the block is
-- scattered to fewer rows while the negated-null-map `filter` kept the full block size. Applying
-- that stale filter to the shrunk left key column raised a size-mismatch LOGICAL_ERROR.

SET enable_analyzer = 1;

-- No matches: every left row gets defaulted right keys. Used to raise
-- "Null map of size N does not match ColumnNullable of size M".
SELECT count(), sum(coalesce(r.a, 0)), sum(coalesce(r.b, 0))
FROM (SELECT number % 50 AS k FROM numbers(100000)) AS l
LEFT JOIN (SELECT toNullable(number % 25) AS a, number % 30 AS b FROM numbers(2)) AS r
  ON (l.k = r.a) AND (l.k > r.b)
SETTINGS join_algorithm = 'hash', join_use_nulls = 1, max_joined_block_size_rows = 256;

-- Some rows match and some do not (the additional filter still shrinks the block), verifying the
-- right key column is joined correctly and not just that the query stops raising.
SELECT count(), sum(coalesce(r.a, 0)), countIf(r.a IS NOT NULL)
FROM (SELECT number % 50 AS k FROM numbers(100000)) AS l
LEFT JOIN (SELECT number * 5 + 5 AS a, 3 AS b FROM numbers(2)) AS r
  ON (l.k = r.a) AND (l.k > r.b)
SETTINGS join_algorithm = 'hash', join_use_nulls = 1, max_joined_block_size_rows = 256;

-- Same shape without join_use_nulls hit the sibling "Invalid number of rows in Chunk" assertion.
SELECT count(), sum(r.a), countIf(r.a > 0)
FROM (SELECT number % 50 AS k FROM numbers(100000)) AS l
LEFT JOIN (SELECT number * 5 + 5 AS a, 3 AS b FROM numbers(2)) AS r
  ON (l.k = r.a) AND (l.k > r.b)
SETTINGS join_algorithm = 'hash', max_joined_block_size_rows = 256;
