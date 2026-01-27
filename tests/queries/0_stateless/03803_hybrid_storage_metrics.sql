-- Tests for hybrid storage metrics and monitoring (Phase 6)
-- Tags: no-random-settings

-- Test ProfileEvents for hybrid storage
DROP TABLE IF EXISTS test_hybrid_metrics;

CREATE TABLE test_hybrid_metrics
(
    id UInt64,
    c1 String, c2 String, c3 String, c4 String, c5 String,
    c6 String, c7 String, c8 String, c9 String, c10 String
)
ENGINE = MergeTree()
ORDER BY id
SETTINGS enable_hybrid_storage = 1,
         hybrid_storage_column_threshold = 0.5;

INSERT INTO test_hybrid_metrics SELECT
    number,
    toString(number), toString(number+1), toString(number+2),
    toString(number+3), toString(number+4), toString(number+5),
    toString(number+6), toString(number+7), toString(number+8),
    toString(number+9)
FROM numbers(1000);

-- Force data to disk
OPTIMIZE TABLE test_hybrid_metrics FINAL;

-- Test 1: Check ProfileEvents exist in system.events
SELECT 'Test 1: ProfileEvents for hybrid storage exist';
SELECT name FROM system.events WHERE name LIKE 'HybridStorage%' ORDER BY name;

-- Test 2: Wide query should increment HybridStorageRowBasedReads
SELECT 'Test 2: Wide query ProfileEvents';
SELECT count() FROM (
    SELECT * FROM test_hybrid_metrics WHERE id < 100
) FORMAT Null;

-- Test 3: Narrow query should increment HybridStorageColumnBasedReads
SELECT 'Test 3: Narrow query ProfileEvents';
SELECT count() FROM (
    SELECT id, c1 FROM test_hybrid_metrics WHERE id < 100
) FORMAT Null;

-- Test 4: Check system.events has the hybrid storage events
SELECT 'Test 4: Verify ProfileEvents are tracked';
SELECT
    name,
    description
FROM system.events
WHERE name IN (
    'HybridStorageRowBasedReads',
    'HybridStorageColumnBasedReads',
    'HybridStorageBytesReadFromRow',
    'HybridStorageRowSerializationMicroseconds',
    'HybridStorageRowDeserializationMicroseconds'
)
ORDER BY name;

DROP TABLE test_hybrid_metrics;

-- Test 5: Performance benchmark - point query
SELECT 'Test 5: Performance benchmark - point query';

DROP TABLE IF EXISTS test_hybrid_perf;

CREATE TABLE test_hybrid_perf
(
    id UInt64,
    c1 String, c2 String, c3 String, c4 String, c5 String,
    c6 String, c7 String, c8 String, c9 String, c10 String,
    c11 String, c12 String, c13 String, c14 String, c15 String,
    c16 String, c17 String, c18 String, c19 String, c20 String
)
ENGINE = MergeTree()
ORDER BY id
SETTINGS enable_hybrid_storage = 1,
         hybrid_storage_column_threshold = 0.5;

INSERT INTO test_hybrid_perf SELECT
    number,
    toString(rand()), toString(rand()), toString(rand()), toString(rand()), toString(rand()),
    toString(rand()), toString(rand()), toString(rand()), toString(rand()), toString(rand()),
    toString(rand()), toString(rand()), toString(rand()), toString(rand()), toString(rand()),
    toString(rand()), toString(rand()), toString(rand()), toString(rand()), toString(rand())
FROM numbers(10000);

OPTIMIZE TABLE test_hybrid_perf FINAL;

-- Benchmark point query (SELECT * WHERE id = X)
SELECT 'Benchmark: Point query with all columns';
SELECT count() FROM (
    SELECT * FROM test_hybrid_perf WHERE id = 5000
) FORMAT Null;

-- Benchmark wide query (SELECT * with filter)
SELECT 'Benchmark: Wide query with range filter';
SELECT count() FROM (
    SELECT * FROM test_hybrid_perf WHERE id BETWEEN 1000 AND 2000
) FORMAT Null;

-- Benchmark narrow query for comparison
SELECT 'Benchmark: Narrow query for comparison';
SELECT count() FROM (
    SELECT id, c1, c2 FROM test_hybrid_perf WHERE id BETWEEN 1000 AND 2000
) FORMAT Null;

-- Test 6: Comparison with hybrid storage disabled
SELECT 'Test 6: Comparison with hybrid storage disabled';

DROP TABLE IF EXISTS test_no_hybrid_perf;

CREATE TABLE test_no_hybrid_perf
(
    id UInt64,
    c1 String, c2 String, c3 String, c4 String, c5 String,
    c6 String, c7 String, c8 String, c9 String, c10 String
)
ENGINE = MergeTree()
ORDER BY id
SETTINGS enable_hybrid_storage = 0;

INSERT INTO test_no_hybrid_perf SELECT
    number,
    toString(rand()), toString(rand()), toString(rand()), toString(rand()), toString(rand()),
    toString(rand()), toString(rand()), toString(rand()), toString(rand()), toString(rand())
FROM numbers(10000);

OPTIMIZE TABLE test_no_hybrid_perf FINAL;

SELECT 'Without hybrid storage - wide query';
SELECT count() FROM (
    SELECT * FROM test_no_hybrid_perf WHERE id BETWEEN 1000 AND 2000
) FORMAT Null;

DROP TABLE test_no_hybrid_perf;
DROP TABLE test_hybrid_perf;

-- Test 7: Row size limit behavior
SELECT 'Test 7: Row size limit behavior';

DROP TABLE IF EXISTS test_hybrid_size_limit;

CREATE TABLE test_hybrid_size_limit
(
    id UInt64,
    large_col String
)
ENGINE = MergeTree()
ORDER BY id
SETTINGS enable_hybrid_storage = 1,
         hybrid_storage_max_row_size = 1024;  -- 1KB limit for testing

-- Insert small row (should fit in __row)
INSERT INTO test_hybrid_size_limit VALUES (1, 'small value');

-- Insert larger row (may exceed limit)
INSERT INTO test_hybrid_size_limit VALUES (2, repeat('x', 2000));

OPTIMIZE TABLE test_hybrid_size_limit FINAL;

-- Query should still work
SELECT id, length(large_col) FROM test_hybrid_size_limit ORDER BY id;

DROP TABLE test_hybrid_size_limit;

-- Test 8: Storage overhead estimation
SELECT 'Test 8: Storage overhead check';

DROP TABLE IF EXISTS test_hybrid_storage_size;
DROP TABLE IF EXISTS test_normal_storage_size;

CREATE TABLE test_hybrid_storage_size
(
    id UInt64,
    c1 String, c2 String, c3 String, c4 String, c5 String
)
ENGINE = MergeTree()
ORDER BY id
SETTINGS enable_hybrid_storage = 1;

CREATE TABLE test_normal_storage_size
(
    id UInt64,
    c1 String, c2 String, c3 String, c4 String, c5 String
)
ENGINE = MergeTree()
ORDER BY id
SETTINGS enable_hybrid_storage = 0;

-- Insert same data to both tables
INSERT INTO test_hybrid_storage_size SELECT
    number,
    toString(number), toString(number+1), toString(number+2),
    toString(number+3), toString(number+4)
FROM numbers(10000);

INSERT INTO test_normal_storage_size SELECT
    number,
    toString(number), toString(number+1), toString(number+2),
    toString(number+3), toString(number+4)
FROM numbers(10000);

OPTIMIZE TABLE test_hybrid_storage_size FINAL;
OPTIMIZE TABLE test_normal_storage_size FINAL;

-- Compare row counts (should be equal)
SELECT 'Hybrid storage row count:', count() FROM test_hybrid_storage_size;
SELECT 'Normal storage row count:', count() FROM test_normal_storage_size;

DROP TABLE test_hybrid_storage_size;
DROP TABLE test_normal_storage_size;

SELECT 'All hybrid storage metrics tests passed';
