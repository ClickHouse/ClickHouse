-- Tags: long, no-random-merge-tree-settings, no-random-settings, no-tsan, no-debug, no-object-storage, no-distributed-cache
-- no-tsan: too slow
-- no-object-storage: for remote tables we use thread pool even when reading with one stream, so memory consumption is higher

SET use_uncompressed_cache = 0;
SET allow_prefetched_read_pool_for_remote_filesystem=0;

DROP TABLE IF EXISTS adaptive_table;

-- If the granularity of consequent blocks differs a lot, then adaptive
-- granularity will adjust the amount of marks correctly.
-- Data for test was empirically derived, it's quite hard to get good parameters.

CREATE TABLE adaptive_table(
    key UInt64,
    value String
) ENGINE MergeTree()
ORDER BY key
SETTINGS index_granularity_bytes = 1048576,
min_bytes_for_wide_part = 0,
min_rows_for_wide_part = 0,
enable_vertical_merge_algorithm = 0;

SET max_block_size=900;

-- There are about 900 marks for our settings.
SET optimize_trivial_insert_select = 1;
INSERT INTO adaptive_table SELECT number, if(number > 700, randomPrintableASCII(102400), randomPrintableASCII(1)) FROM numbers(10000);

OPTIMIZE TABLE adaptive_table FINAL;

SELECT marks FROM system.parts WHERE table = 'adaptive_table' and database=currentDatabase() and active;

SET enable_filesystem_cache = 0;

-- If we have computed granularity incorrectly than we will exceed this limit.
SET max_memory_usage='30M';
SET max_threads = 1;

SELECT max(length(value)) FROM adaptive_table;

DROP TABLE IF EXISTS adaptive_table;
