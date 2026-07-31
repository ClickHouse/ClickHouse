-- A LEFT JOIN whose right table has unique keys is promoted to RightAny. With a non-equi ON
-- condition the join stops collecting candidates once max_joined_block_size_rows is reached, so
-- the left block is truncated to that prefix while the negated null map used to build the
-- required right key column still had the full block size. Applying it to the shorter left key
-- column raised a size-mismatch LOGICAL_ERROR.

-- The old analyzer rejects the non-equi ON condition with INVALID_JOIN_ON_EXPRESSION, and
-- swapping the tables makes this a RIGHT join, which is not promoted to RightAny and never
-- reaches the truncated prefix.
SET enable_analyzer = 1;
SET query_plan_join_swap_table = false;

-- Raised "Null map of size 1000 at offset 0 does not match ColumnNullable of size 256".
SELECT count(), sum(coalesce(r.a, 0)), sum(coalesce(r.b, 0))
FROM (SELECT number % 2 AS k FROM numbers(1000)) AS l
LEFT JOIN (SELECT toNullable(number) AS a, number AS b FROM numbers(2)) AS r
  ON (l.k = r.a) AND (l.k > r.b)
SETTINGS join_algorithm = 'hash', max_joined_block_size_rows = 256;

-- Same shape with a non-nullable right key hit the sibling "Invalid number of rows in Chunk".
SELECT count(), sum(coalesce(r.a, 0)), sum(coalesce(r.b, 0))
FROM (SELECT number % 2 AS k FROM numbers(1000)) AS l
LEFT JOIN (SELECT number AS a, number AS b FROM numbers(2)) AS r
  ON (l.k = r.a) AND (l.k > r.b)
SETTINGS join_algorithm = 'hash', max_joined_block_size_rows = 256;

-- The two queries above join no rows, so also cover the case where the ON condition holds.
SELECT count(), sum(coalesce(r.a, 0)), sum(coalesce(r.b, 0))
FROM (SELECT number % 2 AS k FROM numbers(1000)) AS l
LEFT JOIN (SELECT toNullable(number) AS a, number - 1 AS b FROM numbers(2)) AS r
  ON (l.k = r.a) AND (l.k > r.b)
SETTINGS join_algorithm = 'hash', max_joined_block_size_rows = 256;
