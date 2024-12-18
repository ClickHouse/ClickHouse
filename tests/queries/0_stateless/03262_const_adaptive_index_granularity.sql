DROP TABLE IF EXISTS t_index_granularity;

CREATE TABLE t_index_granularity (id UInt64, s String)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0,
    index_granularity = 10,
    index_granularity_bytes = 4096,
    merge_max_block_size = 10,
    merge_max_block_size_bytes = 4096,
    enable_index_granularity_compression = 1,
    use_const_adaptive_granularity = 0,
    enable_vertical_merge_algorithm = 0;

INSERT INTO t_index_granularity SELECT number, 'a' FROM numbers(15);
INSERT INTO t_index_granularity SELECT number, repeat('a', 2048) FROM numbers(15, 15);

SELECT 'adaptive non-const, before merge';
SELECT * FROM mergeTreeIndex(currentDatabase(), t_index_granularity) ORDER BY ALL;
SELECT name, index_granularity_bytes_in_memory FROM system.parts WHERE database = currentDatabase() AND table = 't_index_granularity' AND active;

OPTIMIZE TABLE t_index_granularity FINAL;

SELECT 'adaptive non-const, after merge';
SELECT * FROM mergeTreeIndex(currentDatabase(), t_index_granularity) ORDER BY ALL;
SELECT name, index_granularity_bytes_in_memory FROM system.parts WHERE database = currentDatabase() AND table = 't_index_granularity' AND active;

DROP TABLE t_index_granularity;

CREATE TABLE t_index_granularity (id UInt64, s String)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0,
    index_granularity = 10,
    index_granularity_bytes = 4096,
    merge_max_block_size = 10,
    merge_max_block_size_bytes = 4096,
    enable_index_granularity_compression = 1,
    use_const_adaptive_granularity = 1,
    enable_vertical_merge_algorithm = 0;

INSERT INTO t_index_granularity SELECT number, 'a' FROM numbers(15);
INSERT INTO t_index_granularity SELECT number, repeat('a', 2048) FROM numbers(15, 15);

SELECT 'adaptive const, before merge';
SELECT * FROM mergeTreeIndex(currentDatabase(), t_index_granularity) ORDER BY ALL;
SELECT name, index_granularity_bytes_in_memory FROM system.parts WHERE database = currentDatabase() AND table = 't_index_granularity' AND active;

OPTIMIZE TABLE t_index_granularity FINAL;

SELECT 'adaptive const, after merge';
SELECT * FROM mergeTreeIndex(currentDatabase(), t_index_granularity) ORDER BY ALL;
SELECT name, index_granularity_bytes_in_memory FROM system.parts WHERE database = currentDatabase() AND table = 't_index_granularity' AND active;

DROP TABLE t_index_granularity;
