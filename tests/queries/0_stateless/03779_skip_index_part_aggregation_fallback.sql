-- Test fallback behavior: compute part-level aggregation from granule-level .idx files
-- when .pidx files don't exist (e.g., old parts created before feature was enabled)

DROP TABLE IF EXISTS test_fallback;

CREATE TABLE test_fallback
(
    key UInt64,
    value Int64,
    INDEX idx_value (value) TYPE minmax GRANULARITY 2
) ENGINE = MergeTree()
ORDER BY key
SETTINGS index_granularity = 4, allow_experimental_skip_index_part_aggregation = 0;

-- For parallel replicas
SET parallel_replicas_local_plan = 1;
SET optimize_aggregation_in_order = 0;

SYSTEM STOP MERGES test_fallback;

-- Part 1: values 0-99
INSERT INTO test_fallback SELECT number, number FROM numbers(100);
-- Part 2: values 1000-1099
INSERT INTO test_fallback SELECT number + 1000, number + 1000 FROM numbers(100);
-- Part 3: values 5000-5099
INSERT INTO test_fallback SELECT number + 5000, number + 5000 FROM numbers(100);

SELECT '--Part count before enabling setting';
SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 'test_fallback' AND active;

ALTER TABLE test_fallback MODIFY SETTING allow_experimental_skip_index_part_aggregation = 1;

-- Detach and attach to reload parts with the new setting
DETACH TABLE test_fallback;
ATTACH TABLE test_fallback;

SYSTEM STOP MERGES test_fallback;

-- Query that should skip all parts (value > 10000, but max value is 5099)
SELECT '--Fallback: Skip all parts';
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_fallback WHERE value > 10000)
WHERE explain LIKE '%ReadFromMergeTree%' OR explain LIKE '%Indexes:%' OR explain LIKE '%SkipIndexPartAgg%' OR explain LIKE '%PrimaryKey%' OR explain LIKE '%Skip%' OR explain LIKE '%Ranges:%' OR match(explain, '^\s+(Description|Parts|Granules|Condition|Name):');
SELECT count() FROM test_fallback WHERE value > 10000;

-- Query that matches only Part 2 (values 1000-1099)
SELECT '--Fallback: Match one part';
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_fallback WHERE value BETWEEN 1050 AND 1060)
WHERE explain LIKE '%ReadFromMergeTree%' OR explain LIKE '%Indexes:%' OR explain LIKE '%SkipIndexPartAgg%' OR explain LIKE '%PrimaryKey%' OR explain LIKE '%Skip%' OR explain LIKE '%Ranges:%' OR match(explain, '^\s+(Description|Parts|Granules|Condition|Name):');
SELECT count() FROM test_fallback WHERE value BETWEEN 1050 AND 1060;

-- Query that should read all parts
SELECT '--Fallback: Read all parts';
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_fallback WHERE value >= 0)
WHERE explain LIKE '%ReadFromMergeTree%' OR explain LIKE '%Indexes:%' OR explain LIKE '%SkipIndexPartAgg%' OR explain LIKE '%PrimaryKey%' OR explain LIKE '%Skip%' OR explain LIKE '%Ranges:%' OR match(explain, '^\s+(Description|Parts|Granules|Condition|Name):');
SELECT count() FROM test_fallback WHERE value >= 0;

-- Insert new data WITH setting enabled - should create .pidx files
INSERT INTO test_fallback SELECT number + 8000, number + 8000 FROM numbers(100);

SELECT '--Mixed: Old parts (fallback) + New part (.pidx)';
SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 'test_fallback' AND active;

-- Query that skips old parts via fallback and new part via .pidx
SELECT '--Mixed: Skip all parts';
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_fallback WHERE value > 20000)
WHERE explain LIKE '%ReadFromMergeTree%' OR explain LIKE '%Indexes:%' OR explain LIKE '%SkipIndexPartAgg%' OR explain LIKE '%PrimaryKey%' OR explain LIKE '%Skip%' OR explain LIKE '%Ranges:%' OR match(explain, '^\s+(Description|Parts|Granules|Condition|Name):');
SELECT count() FROM test_fallback WHERE value > 20000;

-- Query that matches only the new part (8000-8099)
SELECT '--Mixed: Match new part only';
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_fallback WHERE value BETWEEN 8050 AND 8060)
WHERE explain LIKE '%ReadFromMergeTree%' OR explain LIKE '%Indexes:%' OR explain LIKE '%SkipIndexPartAgg%' OR explain LIKE '%PrimaryKey%' OR explain LIKE '%Skip%' OR explain LIKE '%Ranges:%' OR match(explain, '^\s+(Description|Parts|Granules|Condition|Name):');
SELECT count() FROM test_fallback WHERE value BETWEEN 8050 AND 8060;

-- Merge all parts - merged part should have .pidx file
SYSTEM START MERGES test_fallback;
OPTIMIZE TABLE test_fallback FINAL;

SELECT '--After merge';
SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 'test_fallback' AND active;

-- Verify merged part works correctly
SELECT '--Merged: Skip part';
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_fallback WHERE value > 20000)
WHERE explain LIKE '%ReadFromMergeTree%' OR explain LIKE '%Indexes:%' OR explain LIKE '%SkipIndexPartAgg%' OR explain LIKE '%PrimaryKey%' OR explain LIKE '%Skip%' OR explain LIKE '%Ranges:%' OR match(explain, '^\s+(Description|Parts|Granules|Condition|Name):');
SELECT count() FROM test_fallback WHERE value > 20000;

SELECT '--Merged: Use part';
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_fallback WHERE value BETWEEN 1050 AND 5050)
WHERE explain LIKE '%ReadFromMergeTree%' OR explain LIKE '%Indexes:%' OR explain LIKE '%SkipIndexPartAgg%' OR explain LIKE '%PrimaryKey%' OR explain LIKE '%Skip%' OR explain LIKE '%Ranges:%' OR match(explain, '^\s+(Description|Parts|Granules|Condition|Name):');
SELECT count() FROM test_fallback WHERE value BETWEEN 1050 AND 5050;

DROP TABLE test_fallback;
