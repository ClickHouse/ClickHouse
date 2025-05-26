-- Tags: no-parallel
-- Test comprehensive UnionIndex functionality with multiple index types

DROP TABLE IF EXISTS test_union_indexes;

-- Create table with multiple index types
CREATE TABLE test_union_indexes
(
    id UInt64,
    str String,
    num UInt32,
    arr Array(UInt32),
    bloom_str String,
    INDEX idx_bloom bloom_str TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_minmax num TYPE minmax GRANULARITY 1,
    INDEX idx_set str TYPE set(100) GRANULARITY 1,
    INDEX idx_arr_set arr TYPE set(100) GRANULARITY 1
)
ENGINE = MergeTree()
ORDER BY id
SETTINGS index_granularity = 1000;

-- Insert data with specific patterns to test filtering
-- Rows 0-999: bloom_str='value_1', num=1, str='a', arr=[1]
INSERT INTO test_union_indexes SELECT number, 'a', 1, [1], 'value_1' FROM numbers(1000);
-- Rows 1000-1999: bloom_str='value_2', num=2, str='b', arr=[2]
INSERT INTO test_union_indexes SELECT number, 'b', 2, [2], 'value_2' FROM numbers(1000, 1000);
-- Rows 2000-2999: bloom_str='value_3', num=3, str='c', arr=[3]
INSERT INTO test_union_indexes SELECT number, 'c', 3, [3], 'value_3' FROM numbers(2000, 1000);
-- Rows 3000-9999: bloom_str='value_other', num=99, str='z', arr=[99]
INSERT INTO test_union_indexes SELECT number, 'z', 99, [99], 'value_other' FROM numbers(3000, 7000);

-- Test 1: Simple OR with two different index types (bloom filter and minmax)
SELECT 'Test 1: bloom_str OR num';
EXPLAIN indexes = 1 SELECT count() FROM test_union_indexes WHERE bloom_str = 'value_1' OR num = 2;
SELECT count() FROM test_union_indexes WHERE bloom_str = 'value_1' OR num = 2;

-- Test 2: OR with three different index types
SELECT 'Test 2: bloom_str OR num OR str';
EXPLAIN indexes = 1 SELECT count() FROM test_union_indexes WHERE bloom_str = 'value_1' OR num = 2 OR str = 'c';
SELECT count() FROM test_union_indexes WHERE bloom_str = 'value_1' OR num = 2 OR str = 'c';

-- Test 3: OR with all four index types
SELECT 'Test 3: All indexes with OR';
EXPLAIN indexes = 1 SELECT count() FROM test_union_indexes WHERE bloom_str = 'value_1' OR num = 2 OR str = 'c' OR has(arr, 99);
SELECT count() FROM test_union_indexes WHERE bloom_str = 'value_1' OR num = 2 OR str = 'c' OR has(arr, 99);

-- Test 4: Complex nested AND/OR conditions
SELECT 'Test 4: Complex nested conditions';
EXPLAIN indexes = 1 SELECT count() FROM test_union_indexes WHERE (bloom_str = 'value_1' AND id < 500) OR (num = 2 AND id > 1500) OR str = 'c';
SELECT count() FROM test_union_indexes WHERE (bloom_str = 'value_1' AND id < 500) OR (num = 2 AND id > 1500) OR str = 'c';

-- Test 5: Query that filters out most granules (only 1 out of 10 granules match)
SELECT 'Test 5: Highly selective query';
EXPLAIN indexes = 1 SELECT count() FROM test_union_indexes WHERE bloom_str = 'value_1' OR (num = 2 AND id < 1100);
SELECT count() FROM test_union_indexes WHERE bloom_str = 'value_1' OR (num = 2 AND id < 1100);

DROP TABLE test_union_indexes;

-- Test with multiple parts using partitions
DROP TABLE IF EXISTS test_union_parts;

CREATE TABLE test_union_parts
(
    id UInt64,
    part_key UInt32,
    str String,
    num UInt32,
    bloom_str String,
    INDEX idx_bloom bloom_str TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_minmax num TYPE minmax GRANULARITY 1,
    INDEX idx_set str TYPE set(100) GRANULARITY 1
)
ENGINE = MergeTree()
PARTITION BY part_key
ORDER BY id
SETTINGS index_granularity = 1000;

-- Create 5 partitions with different data patterns
-- Partition 0: bloom_str='part0', num=0, str='a' (rows 0-1999)
INSERT INTO test_union_parts SELECT number, 0, 'a', 0, 'part0' FROM numbers(2000);
-- Partition 1: bloom_str='part1', num=1, str='b' (rows 2000-3999)
INSERT INTO test_union_parts SELECT number, 1, 'b', 1, 'part1' FROM numbers(2000, 2000);
-- Partition 2: bloom_str='part2', num=2, str='c' (rows 4000-5999)
INSERT INTO test_union_parts SELECT number, 2, 'c', 2, 'part2' FROM numbers(4000, 2000);
-- Partition 3: bloom_str='other', num=99, str='z' (rows 6000-7999)
INSERT INTO test_union_parts SELECT number, 3, 'z', 99, 'other' FROM numbers(6000, 2000);
-- Partition 4: bloom_str='other', num=99, str='z' (rows 8000-9999)
INSERT INTO test_union_parts SELECT number, 4, 'z', 99, 'other' FROM numbers(8000, 2000);

SELECT 'Test 6: UnionIndex with multiple parts';
SELECT 'Number of parts:', count() FROM system.parts WHERE database = currentDatabase() AND table = 'test_union_parts' AND active;

-- This should filter to only 2 out of 5 parts
EXPLAIN indexes = 1 SELECT count() FROM test_union_parts WHERE bloom_str = 'part0' OR num = 1;
SELECT count() FROM test_union_parts WHERE bloom_str = 'part0' OR num = 1;

-- This should filter to only 3 out of 5 parts
SELECT 'Test 7: Three parts match';
EXPLAIN indexes = 1 SELECT count() FROM test_union_parts WHERE bloom_str = 'part0' OR num = 1 OR str = 'c';
SELECT count() FROM test_union_parts WHERE bloom_str = 'part0' OR num = 1 OR str = 'c';

-- Test with no matching parts
SELECT 'Test 8: No parts match';
EXPLAIN indexes = 1 SELECT count() FROM test_union_parts WHERE bloom_str = 'nonexistent' OR num = 100 OR str = 'nonexistent';
SELECT count() FROM test_union_parts WHERE bloom_str = 'nonexistent' OR num = 100 OR str = 'nonexistent';

DROP TABLE test_union_parts; 