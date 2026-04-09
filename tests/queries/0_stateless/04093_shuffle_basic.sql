-- Basic correctness: all rows are preserved after SHUFFLE.
-- We verify via aggregates that are order-independent.

-- All rows present (count, sum, min, max must match the unshuffled source)
SELECT count(), sum(number), min(number), max(number)
FROM (SELECT number FROM numbers(1000) SHUFFLE);

-- Single row input
SELECT count()
FROM (SELECT number FROM numbers(1) SHUFFLE);

-- Empty input
SELECT count()
FROM (SELECT number FROM numbers(0) SHUFFLE);

-- SHUFFLE with LIMIT: exactly limit rows returned
SELECT count()
FROM (SELECT number FROM numbers(1000) SHUFFLE LIMIT 10);

-- SHUFFLE LIMIT exceeds row count: returns all rows
SELECT count()
FROM (SELECT number FROM numbers(10) SHUFFLE LIMIT 100);

-- SHUFFLE LIMIT 0: returns empty
SELECT count()
FROM (SELECT number FROM numbers(100) SHUFFLE LIMIT 0);

-- SHUFFLE with WHERE (filter first, then shuffle)
SELECT count(), sum(number)
FROM (SELECT number FROM numbers(100) WHERE number >= 50 SHUFFLE);

-- SHUFFLE in subquery with outer ORDER BY (should not interfere)
SELECT count()
FROM (
    SELECT *
    FROM (SELECT number FROM numbers(50) SHUFFLE)
    ORDER BY number
);

-- EXPLAIN PLAN: verify ShuffleStep appears in plan
SELECT countIf(explain LIKE '%Shuffle%') > 0 AS has_shuffle_step
FROM (EXPLAIN PLAN SELECT number FROM numbers(10) SHUFFLE);
