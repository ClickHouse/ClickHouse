-- Comprehensive test cases for LIMIT BY ALL functionality
-- This file covers the most important scenarios in a single test suite

-- Create test table
CREATE TABLE test_limit_by_all_comprehensive (
    id Int32,
    category String,
    value Int32,
    name String,
    score Float64,
    is_active Bool
) ENGINE = Memory;

-- Insert test data
INSERT INTO test_limit_by_all_comprehensive VALUES 
(1, 'A', 100, 'item1', 10.5, true),
(1, 'A', 200, 'item2', 20.3, false),
(1, 'B', 300, 'item3', 15.7, true),
(2, 'A', 400, 'item4', 25.1, true),
(2, 'A', 500, 'item5', 30.2, false),
(2, 'B', 600, 'item6', 35.8, true),
(3, 'C', 700, 'item7', 40.0, true),
(3, 'C', 800, 'item8', 45.2, false),
(4, 'A', 900, 'item9', 50.1, true),
(4, 'B', 1000, 'item10', 55.3, true);

-- Test 1: Basic LIMIT BY ALL functionality
-- Should expand to LIMIT 1 BY id, category
SELECT id, category, value, name
FROM test_limit_by_all_comprehensive 
ORDER BY id, category 
LIMIT 1 BY ALL;

-- Test 2: LIMIT BY ALL with expressions
-- Should expand to LIMIT 1 BY id, category, value
SELECT id, category, value, name, value * 2 as doubled_value
FROM test_limit_by_all_comprehensive 
ORDER BY id, category 
LIMIT 1 BY ALL;

-- Test 3: LIMIT BY ALL with aggregate functions
-- Should expand to LIMIT 1 BY id, category (excluding aggregate functions)
SELECT id, category, 
       count(*) as cnt,
       sum(value) as total_value,
       avg(score) as avg_score
FROM test_limit_by_all_comprehensive 
GROUP BY id, category
ORDER BY id, category 
LIMIT 1 BY ALL;

-- Test 4: LIMIT BY ALL with window functions
-- Should expand to LIMIT 1 BY id, category (excluding window functions)
SELECT id, category, value,
       row_number() OVER (PARTITION BY category ORDER BY value) as rn
FROM test_limit_by_all_comprehensive 
ORDER BY id, category 
LIMIT 1 BY ALL;

-- Test 5: LIMIT BY ALL with complex expressions
-- Should expand to LIMIT 1 BY id, category, value, name
SELECT id, category, value, name,
       CASE WHEN value > 500 THEN 'high' ELSE 'low' END as value_category,
       substring(name, 1, 3) as name_prefix
FROM test_limit_by_all_comprehensive 
ORDER BY id, category 
LIMIT 1 BY ALL;

-- Test 6: LIMIT BY ALL with functions
-- Should expand to LIMIT 1 BY id, category, value, name
SELECT id, category, value, name,
       upper(name) as upper_name,
       value + score as combined_value
FROM test_limit_by_all_comprehensive 
ORDER BY id, category 
LIMIT 1 BY ALL;

-- Test 7: LIMIT BY ALL with conditional logic
-- Should expand to LIMIT 1 BY id, category, value
SELECT id, category, value,
       if(value > 500, 'high', 'low') as value_level
FROM test_limit_by_all_comprehensive 
ORDER BY id, category 
LIMIT 1 BY ALL;

-- Test 8: LIMIT BY ALL with multiple non-aggregate columns
-- Should expand to LIMIT 1 BY id, category, value, name, score, is_active
SELECT id, category, value, name, score, is_active
FROM test_limit_by_all_comprehensive 
ORDER BY id, category 
LIMIT 1 BY ALL;

-- Test 9: LIMIT BY ALL with ORDER BY
-- Should expand to LIMIT 1 BY id, category, value
SELECT id, category, value, name
FROM test_limit_by_all_comprehensive 
ORDER BY value DESC
LIMIT 1 BY ALL;

-- Test 10: LIMIT BY ALL with WHERE clause
-- Should expand to LIMIT 1 BY id, category, value, name
SELECT id, category, value, name
FROM test_limit_by_all_comprehensive 
WHERE value > 500
ORDER BY id, category 
LIMIT 1 BY ALL;

-- Test 11: LIMIT BY ALL with only aggregate functions
-- Should result in empty LIMIT BY clause (no non-aggregate columns)
SELECT count(*) as total_count,
       sum(value) as total_value,
       avg(score) as average_score
FROM test_limit_by_all_comprehensive 
LIMIT 1 BY ALL;

-- Test 12: LIMIT BY ALL with HAVING clause
-- Should expand to LIMIT 1 BY id, category
SELECT id, category,
       count(*) as cnt,
       sum(value) as total_value
FROM test_limit_by_all_comprehensive 
GROUP BY id, category
HAVING count(*) > 1
ORDER BY id, category 
LIMIT 1 BY ALL;

-- Test 13: LIMIT BY ALL with subquery
-- Should expand to LIMIT 1 BY id, category, value
SELECT id, category, value,
       (SELECT count(*) FROM test_limit_by_all_comprehensive t2 WHERE t2.category = test_limit_by_all_comprehensive.category) as category_total
FROM test_limit_by_all_comprehensive 
ORDER BY id, category 
LIMIT 1 BY ALL;

-- Test 14: LIMIT BY ALL with JOIN
-- Should expand to LIMIT 1 BY id, category, value, name
SELECT t1.id, t1.category, t1.value, t1.name,
       t2.value as other_value
FROM test_limit_by_all_comprehensive t1
JOIN test_limit_by_all_comprehensive t2 ON t1.id = t2.id
ORDER BY t1.id, t1.category 
LIMIT 1 BY ALL;

-- Test 15: LIMIT BY ALL with UNION
-- Should expand to LIMIT 1 BY id, category, value, name
SELECT id, category, value, name
FROM test_limit_by_all_comprehensive 
WHERE value < 500
UNION ALL
SELECT id, category, value, name
FROM test_limit_by_all_comprehensive 
WHERE value >= 500
ORDER BY id, category 
LIMIT 1 BY ALL;

-- Test 16: LIMIT BY ALL with CTE
-- Should expand to LIMIT 1 BY id, category, value, name
WITH cte AS (
    SELECT id, category, value, name
    FROM test_limit_by_all_comprehensive 
    WHERE value > 400
)
SELECT id, category, value, name
FROM cte
ORDER BY id, category 
LIMIT 1 BY ALL;

-- Test 17: LIMIT BY ALL with array functions
-- Should expand to LIMIT 1 BY id, category
SELECT id, category,
       count(*) as cnt,
       groupArray(name) as names_array
FROM test_limit_by_all_comprehensive 
GROUP BY id, category
ORDER BY id, category 
LIMIT 1 BY ALL;

-- Test 18: LIMIT BY ALL with string functions
-- Should expand to LIMIT 1 BY id, category, value, name
SELECT id, category, value, name,
       length(name) as name_length,
       concat(category, '_', toString(value)) as combined
FROM test_limit_by_all_comprehensive 
ORDER BY id, category 
LIMIT 1 BY ALL;

-- Test 19: LIMIT BY ALL with mathematical functions
-- Should expand to LIMIT 1 BY id, category, value, score
SELECT id, category, value, score,
       round(score, 1) as rounded_score,
       value * score as product
FROM test_limit_by_all_comprehensive 
ORDER BY id, category 
LIMIT 1 BY ALL;

-- Test 20: LIMIT BY ALL with type casting
-- Should expand to LIMIT 1 BY id, category, value, name
SELECT id, category, value, name,
       toString(value) as value_str,
       toFloat64(value) as value_float
FROM test_limit_by_all_comprehensive 
ORDER BY id, category 
LIMIT 1 BY ALL;

-- Clean up
DROP TABLE test_limit_by_all_comprehensive; 