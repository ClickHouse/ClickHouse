-- Reservoir sampling for SHUFFLE LIMIT k: O(n) time, O(k) space.

-- Returns exactly 5 rows
SELECT count() FROM (SELECT number FROM numbers(1000) SHUFFLE LIMIT 5);

-- Returns exactly 100 rows
SELECT count() FROM (SELECT number FROM numbers(10000) SHUFFLE LIMIT 100);

-- All sampled values are in the valid range [0, 1000)
SELECT countIf(number >= 1000) FROM (SELECT number FROM numbers(1000) SHUFFLE LIMIT 50);

-- LIMIT >= input: returns all rows
SELECT count() FROM (SELECT number FROM numbers(10) SHUFFLE LIMIT 100);

-- LIMIT 0: returns nothing
SELECT count() FROM (SELECT number FROM numbers(100) SHUFFLE LIMIT 0);

-- SHUFFLE LIMIT with OFFSET: count after offset
SELECT count() FROM (SELECT number FROM numbers(1000) SHUFFLE LIMIT 5 OFFSET 3);

-- No duplicates in sample (for small input this is guaranteed)
SELECT count() = countDistinct(number) FROM (SELECT number FROM numbers(100) SHUFFLE LIMIT 50);
