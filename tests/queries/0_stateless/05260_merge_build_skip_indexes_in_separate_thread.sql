-- Tags: no-random-merge-tree-settings
-- no-random-merge-tree-settings: compares the checksums of parts of two tables and checks the merge algorithm.

-- Merges with `merge_build_skip_indexes_in_separate_thread` write the same parts as merges without it,
-- in Horizontal and Vertical merges.

DROP TABLE IF EXISTS t_skip_thread_off;
DROP TABLE IF EXISTS t_skip_thread_on;

CREATE TABLE t_skip_thread_off
(
    id UInt64,
    j JSON(max_dynamic_paths = 4, a UInt64, b String),
    s0 String,
    s1 String,
    INDEX idx_bf s0 TYPE bloom_filter GRANULARITY 1,
    INDEX idx_minmax id TYPE minmax GRANULARITY 2,
    INDEX idx_set s1 TYPE set(100) GRANULARITY 1,
    INDEX idx_json JSONAllPaths(j) TYPE bloom_filter GRANULARITY 1
)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 128, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, enable_vertical_merge_algorithm = 0;

CREATE TABLE t_skip_thread_on AS t_skip_thread_off;
ALTER TABLE t_skip_thread_on MODIFY SETTING merge_build_skip_indexes_in_separate_thread = 1;

SYSTEM STOP MERGES t_skip_thread_off;
SYSTEM STOP MERGES t_skip_thread_on;

INSERT INTO t_skip_thread_off
SELECT number,
    toJSONString(map('a', toString(number), 'b', 'v' || toString(number % 7), 'k' || toString(number % 10), toString(number))),
    toString(cityHash64(number, 0) % 1000), toString(cityHash64(number, 1) % 1000)
FROM numbers(12000)
SETTINGS max_threads = 1, max_insert_threads = 1, max_block_size = 3000, max_insert_block_size = 3000, min_insert_block_size_rows = 0, min_insert_block_size_bytes = 0;

INSERT INTO t_skip_thread_on
SELECT number,
    toJSONString(map('a', toString(number), 'b', 'v' || toString(number % 7), 'k' || toString(number % 10), toString(number))),
    toString(cityHash64(number, 0) % 1000), toString(cityHash64(number, 1) % 1000)
FROM numbers(12000)
SETTINGS max_threads = 1, max_insert_threads = 1, max_block_size = 3000, max_insert_block_size = 3000, min_insert_block_size_rows = 0, min_insert_block_size_bytes = 0;

SYSTEM START MERGES t_skip_thread_off;
SYSTEM START MERGES t_skip_thread_on;
OPTIMIZE TABLE t_skip_thread_off FINAL;
OPTIMIZE TABLE t_skip_thread_on FINAL;

SELECT 'horizontal', count(), uniqExact(hash_of_uncompressed_files), uniqExact(uncompressed_hash_of_compressed_files)
FROM system.parts WHERE database = currentDatabase() AND table IN ('t_skip_thread_off', 't_skip_thread_on') AND active;

-- Rewrite both parts again with a Vertical merge.
ALTER TABLE t_skip_thread_off MODIFY SETTING enable_vertical_merge_algorithm = 1, vertical_merge_algorithm_min_rows_to_activate = 1, vertical_merge_algorithm_min_columns_to_activate = 1;
ALTER TABLE t_skip_thread_on MODIFY SETTING enable_vertical_merge_algorithm = 1, vertical_merge_algorithm_min_rows_to_activate = 1, vertical_merge_algorithm_min_columns_to_activate = 1;
OPTIMIZE TABLE t_skip_thread_off FINAL;
OPTIMIZE TABLE t_skip_thread_on FINAL;

SELECT 'vertical', count(), uniqExact(hash_of_uncompressed_files), uniqExact(uncompressed_hash_of_compressed_files)
FROM system.parts WHERE database = currentDatabase() AND table IN ('t_skip_thread_off', 't_skip_thread_on') AND active;

SYSTEM FLUSH LOGS part_log;
SELECT table, arraySort(groupUniqArray(merge_algorithm)) FROM system.part_log
WHERE database = currentDatabase() AND event_type = 'MergeParts' GROUP BY table ORDER BY table;

SELECT count(), sum(id), groupBitXor(cityHash64(*)) FROM t_skip_thread_on;
SELECT 'bloom_filter', count() FROM t_skip_thread_on WHERE s0 = '42' SETTINGS force_data_skipping_indices = 'idx_bf';
SELECT 'set', count() FROM t_skip_thread_on WHERE s1 = '42' SETTINGS force_data_skipping_indices = 'idx_set';
SELECT 'minmax', count() FROM t_skip_thread_on WHERE id BETWEEN 100 AND 200 SETTINGS force_data_skipping_indices = 'idx_minmax';
SELECT 'json paths', count() FROM t_skip_thread_on WHERE has(JSONAllPaths(j), 'k3') SETTINGS force_data_skipping_indices = 'idx_json';

DROP TABLE t_skip_thread_off;
DROP TABLE t_skip_thread_on;
