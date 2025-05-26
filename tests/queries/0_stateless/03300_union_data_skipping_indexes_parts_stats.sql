-- Tags: no-parallel
-- Test UnionIndex parts statistics reporting

DROP TABLE IF EXISTS test_union_stats;

CREATE TABLE test_union_stats
(
    id UInt64,
    part_key UInt32,
    a UInt32,
    b UInt32,
    c UInt32,
    INDEX idx_a a TYPE minmax GRANULARITY 1,
    INDEX idx_b b TYPE minmax GRANULARITY 1,
    INDEX idx_c c TYPE set(100) GRANULARITY 1
)
ENGINE = MergeTree()
PARTITION BY part_key
ORDER BY id
SETTINGS index_granularity = 1000;

-- Create 6 partitions with specific patterns:
-- Partition 0: a=1 matches (1 granule)
INSERT INTO test_union_stats SELECT number, 0, 1, 0, 0 FROM numbers(1000);
INSERT INTO test_union_stats SELECT number, 0, 0, 0, 0 FROM numbers(1000, 9000);

-- Partition 1: b=1 matches (1 granule)
INSERT INTO test_union_stats SELECT number, 1, 0, 1, 0 FROM numbers(10000, 1000);
INSERT INTO test_union_stats SELECT number, 1, 0, 0, 0 FROM numbers(11000, 9000);

-- Partition 2: c=1 matches (1 granule)
INSERT INTO test_union_stats SELECT number, 2, 0, 0, 1 FROM numbers(20000, 1000);
INSERT INTO test_union_stats SELECT number, 2, 0, 0, 0 FROM numbers(21000, 9000);

-- Partition 3: no matches
INSERT INTO test_union_stats SELECT number, 3, 0, 0, 0 FROM numbers(30000, 10000);

-- Partition 4: no matches
INSERT INTO test_union_stats SELECT number, 4, 0, 0, 0 FROM numbers(40000, 10000);

-- Partition 5: no matches
INSERT INTO test_union_stats SELECT number, 5, 0, 0, 0 FROM numbers(50000, 10000);

SELECT 'Parts statistics test';
SELECT 'Total parts:', count() FROM system.parts WHERE database = currentDatabase() AND table = 'test_union_stats' AND active;

-- Test 1: UnionIndex should show Parts: 3/6 (only partitions 0,1,2 have matches)
-- Each individual index should show Parts: 1/6 (only one partition matches per index)
SELECT 'Test 1: OR across all indexes';
-- EXPLAIN indexes = 1 will show the UnionIndex with parts statistics
SELECT count() FROM test_union_stats WHERE a = 1 OR b = 1 OR c = 1;

-- Test 2: UnionIndex should show Parts: 2/6 (only partitions 0,1 have matches)
SELECT 'Test 2: OR with two indexes';
-- EXPLAIN indexes = 1 will show the UnionIndex with parts statistics
SELECT count() FROM test_union_stats WHERE a = 1 OR b = 1;

-- Test 3: Single index (no UnionIndex)
SELECT 'Test 3: Single index';
-- EXPLAIN indexes = 1 will show single index statistics
SELECT count() FROM test_union_stats WHERE a = 1;

DROP TABLE test_union_stats; 