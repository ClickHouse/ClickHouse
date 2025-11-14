SET max_threads = 1;
SET max_insert_threads = 1;
SET max_block_size = 65536;
SET enable_analyzer = 0;

DROP TABLE IF EXISTS test_limit_by_all_old_planner;

CREATE TABLE test_limit_by_all_old_planner (
    id Int32,
    category String,
    value Int32,
    name String
) ENGINE = Memory;

INSERT INTO test_limit_by_all_old_planner VALUES 
(1, 'A', 100, 'item1'),
(1, 'A', 200, 'item2'),
(1, 'B', 300, 'item3'),
(2, 'A', 400, 'item4'),
(2, 'A', 500, 'item5'),
(2, 'B', 600, 'item6'),
(3, 'C', 700, 'item7');

-- Test 1: Test that LIMIT BY ALL throws an exception when using the old planner
-- This tests the changes in TreeWriter.cpp
SELECT id, category, value, name
FROM test_limit_by_all_old_planner
LIMIT 1 BY ALL
SETTINGS allow_experimental_analyzer = 0; -- {serverError NOT_IMPLEMENTED}

-- Test 2: Basic LIMIT BY usage.
SELECT id, category, value 
FROM test_limit_by_all_old_planner 
ORDER BY id, category, value 
LIMIT 1 BY id, category, value;

-- Test 3: LIMIT BY with computed column - make deterministic by ordering by value
SELECT id, category, concat(category, '_', name) as combined
FROM test_limit_by_all_old_planner 
ORDER BY id, category, value -- Order by value to make deterministic when id, category are same
LIMIT 2 BY id, category, combined;

-- Test 4: LIMIT BY with window function - make deterministic
SELECT id, category, value, row_number() OVER (PARTITION BY category ORDER BY value) AS rn
FROM test_limit_by_all_old_planner
ORDER BY id, category, value, rn 
LIMIT 1 BY id, category, value, rn LIMIT 3;

-- Test 5: LIMIT BY with WHERE clause - make deterministic
SELECT id, category, value 
FROM test_limit_by_all_old_planner 
WHERE value > 200
ORDER BY id, category, value  -- Add value to ordering for deterministic results
LIMIT 1 BY id, category, value;

-- Test 6: LIMIT BY with unique values
SELECT id
FROM (SELECT DISTINCT id, category FROM test_limit_by_all_old_planner)
ORDER BY id, category
LIMIT 1 BY id;

-- Test 7: LIMIT BY with DISTINCT clause
SELECT DISTINCT id, category
FROM test_limit_by_all_old_planner
ORDER BY id, category
LIMIT 1 BY id, category
LIMIT 2;

-- Test 8: Negative LIMIT BY should throw an exception
SELECT id, category, value
FROM test_limit_by_all_old_planner
ORDER BY id, category, value
LIMIT -1 BY id; -- { serverError NOT_IMPLEMENTED }

-- Test 9: LIMIT BY with OFFSET
SELECT id, category
FROM test_limit_by_all_old_planner
ORDER BY id, category, value
LIMIT 2 OFFSET 1 BY id, category;

-- Test 10: LIMIT BY with DESC ORDER BY
SELECT id, category
FROM test_limit_by_all_old_planner
ORDER BY value DESC
LIMIT 1 BY id, category
LIMIT 2;

-- Test 11: 0 LIMIT BY - Should give no result
SELECT id, category, value
FROM test_limit_by_all_old_planner
ORDER BY id, category, value
LIMIT 0 BY id, category, value;

-- Test 12: Misc

SELECT id, category, value
FROM test_limit_by_all_old_planner
ORDER BY id, category, value
LIMIT 2 OFFSET 1 BY id;

SELECT d, category, count() AS c
FROM
(
    WITH toStartOfDay(toDateTime('2025-01-01 12:00:00')) AS d
    SELECT d, category
    FROM test_limit_by_all_old_planner
    ORDER BY d, category, value, name
    LIMIT 2 BY d, category
    SETTINGS enable_positional_arguments = 0
)
GROUP BY d, category
ORDER BY d, category;

-- Test 13: NULL key handling
INSERT INTO test_limit_by_all_old_planner VALUES (4, NULL, 10, 'n1'), (4, NULL, 20, 'n2');

SELECT id, category
FROM test_limit_by_all_old_planner
ORDER BY id, category NULLS FIRST, value
LIMIT 1 BY id, category;

DROP TABLE test_limit_by_all_old_planner; 