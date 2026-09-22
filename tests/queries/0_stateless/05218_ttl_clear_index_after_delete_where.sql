DROP TABLE IF EXISTS ttl_clear_index_after_delete_where;

CREATE TABLE ttl_clear_index_after_delete_where
(
    delete_at Date,
    clear_at Date,
    should_delete UInt8,
    k UInt64,
    v UInt64,
    INDEX idx v TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY k
TTL delete_at + INTERVAL 1 DAY DELETE WHERE should_delete = 1,
    clear_at + INTERVAL 1 DAY CLEAR INDEX idx
SETTINGS
    add_minmax_index_for_numeric_columns = 0,
    add_minmax_index_for_string_columns = 0,
    index_granularity = 2,
    index_granularity_bytes = '10Mi',
    merge_with_ttl_timeout = 86400,
    min_bytes_for_full_part_storage = 0,
    min_rows_for_full_part_storage = 0,
    min_bytes_for_wide_part = 0,
    min_rows_for_wide_part = 0,
    vertical_merge_algorithm_min_rows_to_activate = 100000000;

SYSTEM STOP TTL MERGES ttl_clear_index_after_delete_where;

INSERT INTO ttl_clear_index_after_delete_where VALUES
    ('2100-01-01', '2000-01-01', 0, 1, 1),
    ('2000-01-01', '2100-01-01', 1, 2, 2);

SELECT
    toYear(index_clear_ttl_info.max[1]),
    secondary_indices_compressed_bytes > 0
FROM system.parts
WHERE database = currentDatabase()
  AND table = 'ttl_clear_index_after_delete_where'
  AND active;

SYSTEM START TTL MERGES ttl_clear_index_after_delete_where;
OPTIMIZE TABLE ttl_clear_index_after_delete_where FINAL
SETTINGS
    enable_ttl_clear_index_merge_type_generation = 0,
    optimize_skip_merged_partitions = 0;

SELECT groupArray(k) FROM ttl_clear_index_after_delete_where;

SELECT
    toYear(index_clear_ttl_info.max[1]),
    secondary_indices_compressed_bytes = 0
FROM system.parts
WHERE database = currentDatabase()
  AND table = 'ttl_clear_index_after_delete_where'
  AND active;

DROP TABLE ttl_clear_index_after_delete_where;
