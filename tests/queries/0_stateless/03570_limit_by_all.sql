-- deterministic test for LIMIT BY ALL
SET max_threads = 1;
SET max_insert_threads = 1;
SET max_block_size = 65536;

CREATE TABLE test_limit_by_all (
    id Int32,
    category String,
    value Int32
) ENGINE = Memory;

INSERT INTO test_limit_by_all VALUES
    (1, 'A', 10),
    (1, 'A', 20),
    (2, 'B', 30),
    (2, 'B', 40);

-- 1. Basic LIMIT BY ALL usage
SELECT * FROM test_limit_by_all ORDER BY id, category, value LIMIT 1 BY ALL;

-- 2. LIMIT BY ALL with aggregate functions (should ignore aggregates)
SELECT id, category, count() AS cnt
FROM test_limit_by_all
GROUP BY id, category
ORDER BY id, category LIMIT 1 BY ALL;

-- 3. LIMIT BY ALL with a computed column
SELECT id, category, value, value * 2 AS double_value
FROM test_limit_by_all
ORDER BY id, category, value LIMIT 1 BY ALL;

-- 4. LIMIT BY ALL with a window function
SELECT id, category, value, row_number() OVER (PARTITION BY category ORDER BY value) AS rn
FROM test_limit_by_all
ORDER BY id, category, value, rn LIMIT 1 BY ALL;

-- 5. Edge case: all aggregates (should return a single row)
SELECT count() AS cnt, sum(value) AS total
FROM test_limit_by_all
ORDER BY cnt, total LIMIT 1 BY ALL;

-- Clean up
DROP TABLE test_limit_by_all; 