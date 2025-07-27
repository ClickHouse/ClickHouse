SET max_threads = 1;
SET max_insert_threads = 1;
SET max_block_size = 65536;
SET allow_experimental_analyzer = 1;

CREATE TABLE test_limit_by_all (
    id Int32,
    category String,
    value Int32,
    name String
) ENGINE = Memory;

INSERT INTO test_limit_by_all VALUES 
(1, 'A', 100, 'item1'),
(1, 'A', 200, 'item2'),
(1, 'B', 300, 'item3'),
(2, 'A', 400, 'item4'),
(2, 'A', 500, 'item5'),
(2, 'B', 600, 'item6'),
(3, 'C', 700, 'item7');

-- Test 1: Basic LIMIT BY ALL usage
SELECT id, category, value 
FROM test_limit_by_all 
ORDER BY id, category, value 
LIMIT 1 BY ALL;

-- Test 2: LIMIT BY ALL with aggregate functions (should ignore aggregates)
SELECT id, category, count(*) as cnt
FROM test_limit_by_all 
GROUP BY id, category
ORDER BY id, category 
LIMIT 1 BY ALL;

-- Test 3: LIMIT BY ALL with computed column - make deterministic by ordering by value
SELECT id, category, concat(category, '_', name) as combined
FROM test_limit_by_all 
ORDER BY id, category, value  -- Order by value to make deterministic when id,category are same
LIMIT 2 BY ALL;

-- Test 4: Basic equivalence test
SELECT id, category, value 
FROM test_limit_by_all 
ORDER BY id, category, value 
LIMIT 1 BY ALL;

-- Test 5: Explicit column list (should be equivalent to test 4)
SELECT id, category, value 
FROM test_limit_by_all 
ORDER BY id, category, value 
LIMIT 1 BY id, category, value;

-- Test 6: EXPLAIN SYNTAX test
EXPLAIN SYNTAX 
SELECT id, category, value 
FROM test_limit_by_all 
ORDER BY id, category, value 
LIMIT 1 BY ALL;

-- Test 7: EXPLAIN QUERY TREE test
EXPLAIN QUERY TREE
SELECT id, category, value 
FROM test_limit_by_all 
ORDER BY id, category, value 
LIMIT 1 BY ALL;

-- Test 8: LIMIT BY ALL with window function - make deterministic
SELECT id, category, value, row_number() OVER (PARTITION BY category ORDER BY value) AS rn
FROM test_limit_by_all
ORDER BY id, category, value, rn 
LIMIT 1 BY ALL;

-- Test 9: LIMIT BY ALL with WHERE clause - make deterministic
SELECT id, category, value 
FROM test_limit_by_all 
WHERE value > 200
ORDER BY id, category, value  -- Add value to ordering for deterministic results
LIMIT 1 BY ALL;

DROP TABLE test_limit_by_all;
