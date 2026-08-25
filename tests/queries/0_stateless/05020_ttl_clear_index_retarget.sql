DROP TABLE IF EXISTS ttl_clear_index_retarget;

CREATE TABLE ttl_clear_index_retarget
(
    d Date,
    k UInt64,
    v UInt64,
    INDEX idx_a v TYPE minmax GRANULARITY 1,
    INDEX idx_b k TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY k
TTL d + INTERVAL 1 DAY CLEAR INDEX idx_a
SETTINGS
    index_granularity = 2,
    min_bytes_for_wide_part = 0,
    min_rows_for_wide_part = 0,
    materialize_skip_indexes_on_merge = 1,
    packed_skip_index_max_bytes = 0;

INSERT INTO ttl_clear_index_retarget
SETTINGS materialize_skip_indexes_on_insert = 1
VALUES ('2000-01-01', 1, 1), ('2000-01-01', 2, 2);

OPTIMIZE TABLE ttl_clear_index_retarget FINAL
SETTINGS enable_ttl_clear_index_merge_type_generation = 0;

SELECT name, data_compressed_bytes > 0
FROM system.data_skipping_indices
WHERE database = currentDatabase()
  AND table = 'ttl_clear_index_retarget'
ORDER BY name;

ALTER TABLE ttl_clear_index_retarget
MODIFY TTL d + INTERVAL 1 DAY CLEAR INDEX idx_b
SETTINGS materialize_ttl_after_modify = 0;

OPTIMIZE TABLE ttl_clear_index_retarget FINAL
SETTINGS
    enable_ttl_clear_index_merge_type_generation = 1,
    optimize_skip_merged_partitions = 1;

SELECT name, data_compressed_bytes > 0
FROM system.data_skipping_indices
WHERE database = currentDatabase()
  AND table = 'ttl_clear_index_retarget'
ORDER BY name;

SELECT count() FROM ttl_clear_index_retarget;

DROP TABLE ttl_clear_index_retarget;
