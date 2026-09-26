-- Tags: no-debug, no-tsan, no-msan, no-asan, no-random-settings, no-random-merge-tree-settings

DROP TABLE IF EXISTS t_primary_index_memory;
CREATE TABLE t_primary_index_memory (s String) ENGINE = MergeTree
ORDER BY s SETTINGS index_granularity = 1;

-- The peak tracked memory of this INSERT does not depend on the number of rows: it is bounded by one
-- 1024-row block plus the index of the part being written, so 30000 rows stress the same code path
-- (30 parts, 30000 granules) as 150000 rows did, while writing 5x less data. The larger row count made
-- the test take more than the 180s flaky-check budget when 18 of them run concurrently.
INSERT INTO t_primary_index_memory SELECT repeat('a', 10000) FROM numbers(30000)
SETTINGS
    max_insert_threads = 1,
    max_block_size = 32,
    max_memory_usage = '100M',
    max_insert_block_size = 1024,
    min_insert_block_size_rows = 1024;

SELECT count() FROM t_primary_index_memory;
DROP TABLE t_primary_index_memory;
