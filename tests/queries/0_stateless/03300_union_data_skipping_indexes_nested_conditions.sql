-- Tags: no-parallel
-- Test UnionIndex with complex nested AND/OR conditions

DROP TABLE IF EXISTS test_nested_conditions;

CREATE TABLE test_nested_conditions
(
    id UInt64,
    category String,
    value UInt32,
    tag String,
    flag UInt8,
    INDEX idx_category category TYPE set(100) GRANULARITY 1,
    INDEX idx_value value TYPE minmax GRANULARITY 1,
    INDEX idx_tag tag TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_flag flag TYPE set(10) GRANULARITY 1
)
ENGINE = MergeTree()
ORDER BY id
SETTINGS index_granularity = 1000;

-- Insert data with specific patterns
-- Rows 0-999: category='A', value=10, tag='tag1', flag=1
INSERT INTO test_nested_conditions SELECT number, 'A', 10, 'tag1', 1 FROM numbers(1000);
-- Rows 1000-1999: category='B', value=20, tag='tag2', flag=0
INSERT INTO test_nested_conditions SELECT number, 'B', 20, 'tag2', 0 FROM numbers(1000, 1000);
-- Rows 2000-2999: category='C', value=30, tag='tag3', flag=1
INSERT INTO test_nested_conditions SELECT number, 'C', 30, 'tag3', 1 FROM numbers(2000, 1000);
-- Rows 3000-3999: category='D', value=40, tag='tag4', flag=0
INSERT INTO test_nested_conditions SELECT number, 'D', 40, 'tag4', 0 FROM numbers(3000, 1000);
-- Rows 4000-9999: category='E', value=50, tag='tag5', flag=1
INSERT INTO test_nested_conditions SELECT number, 'E', 50, 'tag5', 1 FROM numbers(4000, 6000);

-- Test 1: Simple nested condition (A AND id<500) OR (B AND id>1500)
SELECT 'Test 1: (category=A AND id<500) OR (category=B AND id>1500)';
SELECT count() FROM test_nested_conditions WHERE (category = 'A' AND id < 500) OR (category = 'B' AND id > 1500);

-- Test 2: Three-way nested OR with AND conditions
SELECT 'Test 2: Complex three-way nested';
SELECT count() FROM test_nested_conditions WHERE 
    (category = 'A' AND value = 10 AND flag = 1) OR 
    (tag = 'tag2' AND value = 20 AND id < 1500) OR 
    (category = 'C' AND flag = 1);

-- Test 3: Deeply nested conditions
SELECT 'Test 3: Deeply nested conditions';
SELECT count() FROM test_nested_conditions WHERE 
    ((category = 'A' OR category = 'B') AND value < 25) OR 
    ((tag = 'tag3' OR tag = 'tag4') AND id > 2500) OR 
    (flag = 1 AND value = 50);

-- Test 4: Mix of positive and negative conditions
SELECT 'Test 4: Mix of positive and negative conditions';
SELECT count() FROM test_nested_conditions WHERE 
    (category = 'A' AND value != 20) OR 
    (tag = 'tag2' AND flag != 1) OR 
    (value > 40 AND category != 'D');

-- Test 5: Condition that filters out most data (highly selective)
SELECT 'Test 5: Highly selective nested condition';
SELECT count() FROM test_nested_conditions WHERE 
    (category = 'A' AND id < 100) OR 
    (tag = 'tag3' AND id > 2900) OR 
    (value = 40 AND flag = 0 AND id < 3100);

-- Test 6: All conditions false (no matches)
SELECT 'Test 6: No matches';
SELECT count() FROM test_nested_conditions WHERE 
    (category = 'Z' AND value = 100) OR 
    (tag = 'nonexistent' AND flag = 2) OR 
    (value > 100 AND id < 0);

DROP TABLE test_nested_conditions; 