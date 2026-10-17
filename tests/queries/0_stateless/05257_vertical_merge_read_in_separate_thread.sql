-- Tags: no-random-merge-tree-settings
-- no-random-merge-tree-settings: compares the checksums of parts of two tables and checks the merge algorithm.

-- A Vertical merge with `vertical_merge_read_in_separate_thread` writes the same part as a merge without it.

DROP TABLE IF EXISTS t_read_thread_off;
DROP TABLE IF EXISTS t_read_thread_on;

CREATE TABLE t_read_thread_off
(
    id UInt64,
    j JSON(max_dynamic_paths = 4, a UInt64, b String),
    s0 String,
    s1 String,
    arr Array(String),
    INDEX idx_bf s0 TYPE bloom_filter GRANULARITY 1
)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 128, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
    enable_vertical_merge_algorithm = 1, vertical_merge_algorithm_min_rows_to_activate = 1, vertical_merge_algorithm_min_columns_to_activate = 1;

CREATE TABLE t_read_thread_on AS t_read_thread_off;
ALTER TABLE t_read_thread_on MODIFY SETTING vertical_merge_read_in_separate_thread = 1;

SYSTEM STOP MERGES t_read_thread_off;
SYSTEM STOP MERGES t_read_thread_on;

INSERT INTO t_read_thread_off
SELECT number,
    toJSONString(map('a', toString(number), 'b', 'v' || toString(number % 7), 'k' || toString(number % 10), toString(number))),
    toString(cityHash64(number, 0) % 1000), toString(cityHash64(number, 1) % 1000), [toString(number), toString(number % 3)]
FROM numbers(12000)
SETTINGS max_threads = 1, max_insert_threads = 1, max_block_size = 3000, max_insert_block_size = 3000, min_insert_block_size_rows = 0, min_insert_block_size_bytes = 0;

INSERT INTO t_read_thread_on
SELECT number,
    toJSONString(map('a', toString(number), 'b', 'v' || toString(number % 7), 'k' || toString(number % 10), toString(number))),
    toString(cityHash64(number, 0) % 1000), toString(cityHash64(number, 1) % 1000), [toString(number), toString(number % 3)]
FROM numbers(12000)
SETTINGS max_threads = 1, max_insert_threads = 1, max_block_size = 3000, max_insert_block_size = 3000, min_insert_block_size_rows = 0, min_insert_block_size_bytes = 0;

SYSTEM START MERGES t_read_thread_off;
SYSTEM START MERGES t_read_thread_on;
OPTIMIZE TABLE t_read_thread_off FINAL;
OPTIMIZE TABLE t_read_thread_on FINAL;

SELECT count(), uniqExact(hash_of_uncompressed_files), uniqExact(uncompressed_hash_of_compressed_files)
FROM system.parts WHERE database = currentDatabase() AND table IN ('t_read_thread_off', 't_read_thread_on') AND active;

SYSTEM FLUSH LOGS part_log;
SELECT DISTINCT table, merge_algorithm FROM system.part_log
WHERE database = currentDatabase() AND event_type = 'MergeParts' ORDER BY table;

SELECT count(), sum(id), groupBitXor(cityHash64(*)) FROM t_read_thread_on;
SELECT count() FROM t_read_thread_on WHERE s0 = '42' SETTINGS force_data_skipping_indices = 'idx_bf';

DROP TABLE t_read_thread_off;
DROP TABLE t_read_thread_on;
