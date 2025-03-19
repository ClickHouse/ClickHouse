-- Tags: long, no-random-merge-tree-settings

DROP TABLE IF EXISTS t_vertical_merge_memory;

CREATE TABLE t_vertical_merge_memory (id UInt64, arr Array(String))
ENGINE = MergeTree ORDER BY id
SETTINGS
    min_bytes_for_wide_part = 0,
    vertical_merge_algorithm_min_rows_to_activate = 1,
    vertical_merge_algorithm_min_columns_to_activate = 1,
    index_granularity = 8192,
    index_granularity_bytes = '10M',
    merge_max_block_size = 8192,
    merge_max_block_size_bytes = '10M';

INSERT INTO t_vertical_merge_memory SELECT number, arrayMap(x -> repeat('a', 50), range(1000)) FROM numbers(3000);
-- Why 3001? - Deduplication, which is off with normal MergeTree by default but on for ReplicatedMergeTree and SharedMergeTree.
-- We automatically replace MergeTree with SharedMergeTree in ClickHouse Cloud.
INSERT INTO t_vertical_merge_memory SELECT number, arrayMap(x -> repeat('a', 50), range(1000)) FROM numbers(3001);

OPTIMIZE TABLE t_vertical_merge_memory FINAL;

SYSTEM FLUSH LOGS part_log;

SELECT
    merge_algorithm,
    peak_memory_usage < 500 * 1024 * 1024
        ? 'OK'
        : format('FAIL: memory usage: {}', formatReadableSize(peak_memory_usage))
FROM system.part_log
WHERE
    database = currentDatabase()
    AND table = 't_vertical_merge_memory'
    AND event_type = 'MergeParts'
    AND length(merged_from) = 2;

DROP TABLE IF EXISTS t_vertical_merge_memory;
