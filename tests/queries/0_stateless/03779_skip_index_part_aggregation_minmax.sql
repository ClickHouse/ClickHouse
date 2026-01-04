DROP TABLE IF EXISTS test_skip_idx_part_agg;

CREATE TABLE test_skip_idx_part_agg
(
    key UInt64,
    value Int64,
    value_nullable Nullable(Int64),
    extra Int64,
    INDEX idx_value (value) TYPE minmax GRANULARITY 2,
    INDEX idx_value_nullable (value_nullable) TYPE minmax GRANULARITY 2
) ENGINE = MergeTree()
ORDER BY key
SETTINGS index_granularity = 4, allow_experimental_skip_index_part_aggregation = 1;

-- For parallel replicas
SET parallel_replicas_local_plan = 1;
SET optimize_aggregation_in_order = 0;

SYSTEM STOP MERGES test_skip_idx_part_agg;

-- Part 1: values 0-99, nullable with some NULLs
INSERT INTO test_skip_idx_part_agg SELECT number, number, if(number % 10 = 0, NULL, number), number * 2 FROM numbers(100);
-- Part 2: values 1000-1099
INSERT INTO test_skip_idx_part_agg SELECT number + 1000, number + 1000, number + 1000, (number + 1000) * 2 FROM numbers(100);
-- Part 3: values 5000-5099
INSERT INTO test_skip_idx_part_agg SELECT number + 5000, number + 5000, number + 5000, (number + 5000) * 2 FROM numbers(100);

SELECT '--Basic part count';
SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 'test_skip_idx_part_agg' AND active;

-- Query that should skip all parts based on part-level aggregation
SELECT '--Skip all parts';
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_skip_idx_part_agg WHERE value > 10000)
WHERE explain LIKE '%ReadFromMergeTree%' OR explain LIKE '%Indexes:%' OR explain LIKE '%SkipIndexPartAgg%' OR explain LIKE '%PrimaryKey%' OR explain LIKE '%Skip%' OR explain LIKE '%Ranges:%' OR match(explain, '^\s+(Description|Parts|Granules|Condition|Name):');
SELECT count() FROM test_skip_idx_part_agg WHERE value > 10000;

-- Query that matches only one part
SELECT '--Match one part';
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_skip_idx_part_agg WHERE value BETWEEN 1050 AND 1060)
WHERE explain LIKE '%ReadFromMergeTree%' OR explain LIKE '%Indexes:%' OR explain LIKE '%SkipIndexPartAgg%' OR explain LIKE '%PrimaryKey%' OR explain LIKE '%Skip%' OR explain LIKE '%Ranges:%' OR match(explain, '^\s+(Description|Parts|Granules|Condition|Name):');
SELECT count() FROM test_skip_idx_part_agg WHERE value BETWEEN 1050 AND 1060;

-- Query that should read all parts
SELECT '--Read all parts';
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_skip_idx_part_agg WHERE value >= 0)
WHERE explain LIKE '%ReadFromMergeTree%' OR explain LIKE '%Indexes:%' OR explain LIKE '%SkipIndexPartAgg%' OR explain LIKE '%PrimaryKey%' OR explain LIKE '%Skip%' OR explain LIKE '%Ranges:%' OR match(explain, '^\s+(Description|Parts|Granules|Condition|Name):');
SELECT count() FROM test_skip_idx_part_agg WHERE value >= 0;

-- Test nullable column
SELECT '--Nullable - Skip all parts';
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_skip_idx_part_agg WHERE value_nullable > 10000)
WHERE explain LIKE '%ReadFromMergeTree%' OR explain LIKE '%Indexes:%' OR explain LIKE '%SkipIndexPartAgg%' OR explain LIKE '%PrimaryKey%' OR explain LIKE '%Skip%' OR explain LIKE '%Ranges:%' OR match(explain, '^\s+(Description|Parts|Granules|Condition|Name):');
SELECT count() FROM test_skip_idx_part_agg WHERE value_nullable > 10000;

SELECT '--Nullable - Match one part';
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_skip_idx_part_agg WHERE value_nullable BETWEEN 1050 AND 1060)
WHERE explain LIKE '%ReadFromMergeTree%' OR explain LIKE '%Indexes:%' OR explain LIKE '%SkipIndexPartAgg%' OR explain LIKE '%PrimaryKey%' OR explain LIKE '%Skip%' OR explain LIKE '%Ranges:%' OR match(explain, '^\s+(Description|Parts|Granules|Condition|Name):');
SELECT count() FROM test_skip_idx_part_agg WHERE value_nullable BETWEEN 1050 AND 1060;

-- Test merge handling
SYSTEM START MERGES test_skip_idx_part_agg;
OPTIMIZE TABLE test_skip_idx_part_agg FINAL;

SELECT '--Parts after merge';
SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 'test_skip_idx_part_agg' AND active;

-- Verify merged part still has correct aggregation
SELECT '--Skip merged part';
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_skip_idx_part_agg WHERE value > 10000)
WHERE explain LIKE '%ReadFromMergeTree%' OR explain LIKE '%Indexes:%' OR explain LIKE '%SkipIndexPartAgg%' OR explain LIKE '%PrimaryKey%' OR explain LIKE '%Skip%' OR explain LIKE '%Ranges:%' OR match(explain, '^\s+(Description|Parts|Granules|Condition|Name):');
SELECT count() FROM test_skip_idx_part_agg WHERE value > 10000;

SELECT '--Use merged part';
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_skip_idx_part_agg WHERE value BETWEEN 50 AND 1050)
WHERE explain LIKE '%ReadFromMergeTree%' OR explain LIKE '%Indexes:%' OR explain LIKE '%SkipIndexPartAgg%' OR explain LIKE '%PrimaryKey%' OR explain LIKE '%Skip%' OR explain LIKE '%Ranges:%' OR match(explain, '^\s+(Description|Parts|Granules|Condition|Name):');
SELECT count() FROM test_skip_idx_part_agg WHERE value BETWEEN 50 AND 1050;

-- Test MATERIALIZE INDEX - add new index on extra column
ALTER TABLE test_skip_idx_part_agg ADD INDEX idx_extra (extra) TYPE minmax GRANULARITY 2;
ALTER TABLE test_skip_idx_part_agg MATERIALIZE INDEX idx_extra SETTINGS mutations_sync = 2;

-- Verify new index works with part aggregation
SELECT '--After MATERIALIZE - Skip part';
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_skip_idx_part_agg WHERE extra > 20000)
WHERE explain LIKE '%ReadFromMergeTree%' OR explain LIKE '%Indexes:%' OR explain LIKE '%SkipIndexPartAgg%' OR explain LIKE '%PrimaryKey%' OR explain LIKE '%Skip%' OR explain LIKE '%Ranges:%' OR match(explain, '^\s+(Description|Parts|Granules|Condition|Name):');
SELECT count() FROM test_skip_idx_part_agg WHERE extra > 20000;

SELECT '--After MATERIALIZE - Match range';
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM test_skip_idx_part_agg WHERE extra BETWEEN 2100 AND 2120)
WHERE explain LIKE '%ReadFromMergeTree%' OR explain LIKE '%Indexes:%' OR explain LIKE '%SkipIndexPartAgg%' OR explain LIKE '%PrimaryKey%' OR explain LIKE '%Skip%' OR explain LIKE '%Ranges:%' OR match(explain, '^\s+(Description|Parts|Granules|Condition|Name):');
SELECT count() FROM test_skip_idx_part_agg WHERE extra BETWEEN 2100 AND 2120;

DROP TABLE test_skip_idx_part_agg;
