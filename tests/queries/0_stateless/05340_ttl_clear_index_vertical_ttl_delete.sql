-- A vertical TTL-delete merge evaluates every TTL rule against the merged block, so the column of a
-- `CLEAR INDEX` rule must be merged, not gathered. The rows TTL reads `d` and the `CLEAR INDEX` rule
-- reads `e`, so neither column is merged because the other rule needs it.

DROP TABLE IF EXISTS ttl_clear_index_vertical;

CREATE TABLE ttl_clear_index_vertical
(
    id UInt64,
    d DateTime,
    e DateTime,
    c1 UInt64,
    c2 UInt64,
    INDEX idx c1 TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
TTL d + INTERVAL 1 DAY DELETE, e + INTERVAL 1 DAY CLEAR INDEX idx
SETTINGS
    min_bytes_for_wide_part = 0,
    min_bytes_for_full_part_storage = 0,
    enable_block_number_column = 0,
    enable_block_offset_column = 0,
    vertical_merge_algorithm_min_rows_to_activate = 1,
    vertical_merge_algorithm_min_columns_to_activate = 1,
    vertical_merge_optimize_ttl_delete = 1,
    ratio_of_defaults_for_sparse_serialization = 1.0,
    max_bytes_to_merge_at_max_space_in_pool = 1;

-- The table has two parts, so the merge reads through the merging algorithm. The size limit keeps
-- background merges off the parts. Even rows are past the rows TTL, and every row is past the
-- `CLEAR INDEX` rule.
SYSTEM STOP TTL MERGES ttl_clear_index_vertical;
INSERT INTO ttl_clear_index_vertical
SELECT number, if(number % 2, now() + INTERVAL 1 YEAR, '2000-01-01 00:00:00'), '2000-01-01 00:00:00', 1, 1 FROM numbers(100);
INSERT INTO ttl_clear_index_vertical
SELECT number + 100, if(number % 2, now() + INTERVAL 1 YEAR, '2000-01-01 00:00:00'), '2000-01-01 00:00:00', 2, 2 FROM numbers(100);
SYSTEM START TTL MERGES ttl_clear_index_vertical;

OPTIMIZE TABLE ttl_clear_index_vertical FINAL;

SELECT count(), min(id), max(id), countIf(id % 2 = 1), sum(c1), sum(c2) FROM ttl_clear_index_vertical;
SELECT data_compressed_bytes FROM system.data_skipping_indices
WHERE database = currentDatabase() AND table = 'ttl_clear_index_vertical';

SYSTEM FLUSH LOGS part_log;
SELECT merge_algorithm FROM system.part_log
WHERE database = currentDatabase() AND table = 'ttl_clear_index_vertical' AND event_type = 'MergeParts';

DROP TABLE ttl_clear_index_vertical;
