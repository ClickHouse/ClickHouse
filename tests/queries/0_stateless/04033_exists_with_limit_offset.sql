-- Regression test: EXISTS should respect LIMIT and OFFSET in subqueries.
-- If the offset skips all rows, EXISTS should return 0.

-- The subquery itself returns nothing when offset skips all rows
SELECT 1 LIMIT 1 OFFSET 1;

-- Empty result, EXISTS returns 0
SELECT EXISTS (SELECT 1 LIMIT 1 OFFSET 1);

-- Non-empty result, EXISTS returns 1
SELECT EXISTS (SELECT 1 LIMIT 1 OFFSET 0);
SELECT EXISTS (SELECT 1);

-- Offset beyond the result set, EXISTS returns 0
SELECT EXISTS (SELECT number FROM numbers(5) LIMIT 3 OFFSET 10);
-- Offset within range, EXISTS returns 1
SELECT EXISTS (SELECT number FROM numbers(5) LIMIT 3 OFFSET 2);
-- LIMIT 0 always yields an empty set, EXISTS returns 0
SELECT EXISTS (SELECT 1 LIMIT 0);
