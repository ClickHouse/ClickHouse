-- Tags: no-debug, no-tsan, no-msan, no-asan, no-random-settings, no-random-merge-tree-settings

DROP TABLE IF EXISTS t_primary_index_memory;
CREATE TABLE t_primary_index_memory (s String) ENGINE = MergeTree
ORDER BY s SETTINGS index_granularity = 1;

INSERT INTO t_primary_index_memory SELECT repeat('a', 10000) FROM numbers(150000)
SETTINGS
    max_block_size = 32,
    max_memory_usage = '100M',
    max_insert_block_size = 1024,
    min_insert_block_size_rows = 1024;

SELECT count() FROM t_primary_index_memory;
DROP TABLE t_primary_index_memory;
