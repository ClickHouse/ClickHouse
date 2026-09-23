DROP TABLE IF EXISTS test_auto_stats_replacing;

CREATE TABLE test_auto_stats_replacing
(
    id UInt64,
    val UInt64
)
ENGINE = ReplacingMergeTree
ORDER BY id
SETTINGS
    enable_block_number_column = 0,
    enable_block_offset_column = 0,
    auto_statistics_types = '';

SYSTEM STOP MERGES test_auto_stats_replacing;

-- First insert: keys 0..999, val = number (range 0..999, 1000 distinct values)
INSERT INTO test_auto_stats_replacing SELECT number, number FROM numbers(1000);
-- Second insert: keys 500..1499, val = 42 (constant, overwrites keys 500..999 upon merge)
INSERT INTO test_auto_stats_replacing SELECT number + 500, 42 FROM numbers(1000);

SELECT 'after inserts';

SELECT name, column, type, statistics, estimates.cardinality, estimates.min, estimates.max
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 'test_auto_stats_replacing' AND active
ORDER BY name, column;

ALTER TABLE test_auto_stats_replacing MODIFY SETTING auto_statistics_types = 'uniq,minmax,tdigest';

-- Build statistics on merge when source parts doesn't have statistics.
SYSTEM START MERGES test_auto_stats_replacing;
OPTIMIZE TABLE test_auto_stats_replacing FINAL;

SELECT 'after opitmize final';

SELECT name, column, type, statistics, estimates.cardinality, estimates.min, estimates.max
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 'test_auto_stats_replacing' AND active
ORDER BY name, column;

DROP TABLE test_auto_stats_replacing;

CREATE TABLE test_auto_stats_replacing
(
    id UInt64,
    val UInt64
)
ENGINE = ReplacingMergeTree
ORDER BY id
SETTINGS
    enable_block_number_column = 0,
    enable_block_offset_column = 0,
    auto_statistics_types = 'uniq,minmax,tdigest';

SYSTEM STOP MERGES test_auto_stats_replacing;

-- First insert: keys 0..999, val = number (range 0..999, 1000 distinct values)
INSERT INTO test_auto_stats_replacing SELECT number, number FROM numbers(1000);
-- Second insert: keys 500..1499, val = 42 (constant, overwrites keys 500..999 upon merge)
INSERT INTO test_auto_stats_replacing SELECT number + 500, 42 FROM numbers(1000);

SELECT 'after inserts';

SELECT name, column, type, statistics, estimates.cardinality, estimates.min, estimates.max
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 'test_auto_stats_replacing' AND active
ORDER BY name, column;

-- Build statistics on merge when source parts have statistics.
-- Check that statistics are built on actual data, not merged from source parts.
SYSTEM START MERGES test_auto_stats_replacing;
OPTIMIZE TABLE test_auto_stats_replacing FINAL;

SELECT 'after opitmize final';

SELECT name, column, type, statistics, estimates.cardinality, estimates.min, estimates.max
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 'test_auto_stats_replacing' AND active
ORDER BY name, column;

DROP TABLE test_auto_stats_replacing;
