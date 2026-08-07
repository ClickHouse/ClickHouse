-- Tags: no-fasttest

DROP TABLE IF EXISTS 03250_avoid_prefetch;
CREATE table 03250_avoid_prefetch(id UInt64, string LowCardinality(String))
ENGINE = MergeTree
ORDER BY id
SETTINGS enable_vertical_merge_algorithm = 1, vertical_merge_algorithm_min_rows_to_activate = 1, vertical_merge_remote_filesystem_prefetch = 1,
vertical_merge_algorithm_min_bytes_to_activate = 1, vertical_merge_algorithm_min_columns_to_activate = 1,
min_rows_for_wide_part = 1, min_bytes_for_wide_part = 1, remove_empty_parts = 0, storage_policy = 's3_no_cache';

INSERT INTO 03250_avoid_prefetch VALUES (1, 'test');
ALTER TABLE 03250_avoid_prefetch DELETE WHERE id = 1;
INSERT INTO 03250_avoid_prefetch VALUES (2, 'test');
OPTIMIZE TABLE 03250_avoid_prefetch FINAL;

