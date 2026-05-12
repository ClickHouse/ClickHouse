-- PR 6: Comprehensive SHUFFLE tests — correctness, edge cases, interactions.

-- ============================================================
-- Row preservation
-- ============================================================

-- All rows present, all values intact
SELECT count(), sum(number), min(number), max(number)
FROM (SELECT number FROM numbers(10000) SHUFFLE);

-- No duplicates introduced
SELECT count() = countDistinct(number) AS no_dupes
FROM (SELECT number FROM numbers(1000) SHUFFLE);

-- String type preserved
SELECT count(), min(s), max(s)
FROM (SELECT toString(number) AS s FROM numbers(100) SHUFFLE);

-- Float64 type preserved (sum is deterministic)
SELECT count(), round(sum(x), 2)
FROM (SELECT number / 3.0 AS x FROM numbers(30) SHUFFLE);

-- Nullable type preserved
SELECT count(), countIf(x IS NULL) AS nulls
FROM (SELECT if(number % 5 = 0, NULL, number) AS x FROM numbers(20) SHUFFLE);

-- ============================================================
-- Edge cases
-- ============================================================

-- Empty input
SELECT count() FROM (SELECT number FROM numbers(0) SHUFFLE);

-- Single row
SELECT number FROM numbers(1) SHUFFLE;

-- Two rows — both present
SELECT count(), sum(number) FROM (SELECT number FROM numbers(2) SHUFFLE);

-- Large input (verify no crash, check sum)
SELECT count(), sum(number) FROM (SELECT number FROM numbers(100000) SHUFFLE);

-- ============================================================
-- SHUFFLE LIMIT: Correctness
-- ============================================================

-- Exact count
SELECT count() FROM (SELECT number FROM numbers(10000) SHUFFLE LIMIT 50);

-- All values in valid range
SELECT min(number) >= 0 AND max(number) < 10000 AS in_range
FROM (SELECT number FROM numbers(10000) SHUFFLE LIMIT 100);

-- No duplicates in sample
SELECT count() = countDistinct(number) AS no_dupes
FROM (SELECT number FROM numbers(10000) SHUFFLE LIMIT 500);

-- LIMIT > total rows: returns all
SELECT count() FROM (SELECT number FROM numbers(10) SHUFFLE LIMIT 1000);

-- LIMIT 0
SELECT count() FROM (SELECT number FROM numbers(100) SHUFFLE LIMIT 0);

-- LIMIT 1
SELECT count() FROM (SELECT number FROM numbers(1000) SHUFFLE LIMIT 1);

-- LIMIT equals total rows
SELECT count(), sum(number) FROM (SELECT number FROM numbers(50) SHUFFLE LIMIT 50);

-- SHUFFLE LIMIT with OFFSET
SELECT count() FROM (SELECT number FROM numbers(1000) SHUFFLE LIMIT 10 OFFSET 5);

-- ============================================================
-- Interactions with other clauses
-- ============================================================

-- SHUFFLE + WHERE
SELECT count(), sum(number)
FROM (SELECT number FROM numbers(100) WHERE number >= 50 SHUFFLE);

-- SHUFFLE + GROUP BY
SELECT count()
FROM (SELECT number % 10 AS n, count() AS c FROM numbers(100) GROUP BY n SHUFFLE);

-- SHUFFLE + HAVING
SELECT count()
FROM (SELECT number % 10 AS n, count() AS c FROM numbers(100) GROUP BY n HAVING c >= 10 SHUFFLE);

-- SHUFFLE + DISTINCT
SELECT count()
FROM (SELECT DISTINCT number % 20 AS n FROM numbers(100) SHUFFLE);

-- SHUFFLE in subquery, outer has ORDER BY (outer scope independent)
SELECT n FROM (SELECT number AS n FROM numbers(5) SHUFFLE) ORDER BY n;

-- SHUFFLE + CTE
WITH t AS (SELECT number FROM numbers(50) SHUFFLE)
SELECT count(), sum(number) FROM t;

-- SHUFFLE + JOIN
SELECT count()
FROM (
    SELECT a.number
    FROM (SELECT number FROM numbers(10)) AS a
    JOIN (SELECT number FROM numbers(10)) AS b ON a.number = b.number
    SHUFFLE
);

-- SHUFFLE + UNION ALL (each branch independent)
SELECT count()
FROM (
    SELECT number FROM numbers(10) SHUFFLE
    UNION ALL
    SELECT number + 100 FROM numbers(10) SHUFFLE
);

-- ============================================================
-- Error paths
-- ============================================================

-- SHUFFLE + ORDER BY → syntax error
SELECT 1 FROM numbers(1) ORDER BY number SHUFFLE; -- { serverError SYNTAX_ERROR }
SELECT 1 FROM numbers(1) SHUFFLE ORDER BY number; -- { serverError SYNTAX_ERROR }

-- ============================================================
-- EXPLAIN shows correct steps
-- ============================================================

SELECT count() > 0 AS has_shuffle
FROM (EXPLAIN PLAN SELECT number FROM numbers(100) SHUFFLE)
WHERE explain LIKE '%Shuffle%';

SELECT count() > 0 AS has_transform
FROM (EXPLAIN PIPELINE SELECT number FROM numbers(100) SHUFFLE)
WHERE explain LIKE '%ShuffleTransform%';
