-- Tags: no-replicated-database, no-shared-merge-tree
-- no-replicated-database, no-shared-merge-tree: `OPTIMIZE` without `FINAL` selects TTL merges only on plain `MergeTree`.

-- A TTL merge that deletes rows recalculates the TTL info, and here the index expires only through
-- that recalculation. The merge then removes the index files after writing the part. Both indexes are in
-- `skp_idx.packed`, and only `idx_w` expires.

DROP TABLE IF EXISTS ttl_clear_index_after_delete_where;

CREATE TABLE ttl_clear_index_after_delete_where
(
    delete_at Date,
    clear_at Date,
    should_delete UInt8,
    k UInt64,
    v UInt64,
    w UInt64,
    INDEX idx_v v TYPE minmax GRANULARITY 1,
    INDEX idx_w w TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY k
TTL delete_at + INTERVAL 1 DAY DELETE WHERE should_delete = 1,
    clear_at + INTERVAL 1 DAY CLEAR INDEX idx_w,
    delete_at + INTERVAL 1 DAY CLEAR INDEX idx_v
SETTINGS
    packed_skip_index_max_bytes = '1Mi',
    index_granularity = 2,
    index_granularity_bytes = '10Mi',
    min_bytes_for_wide_part = 0,
    min_bytes_for_full_part_storage = 0,
    vertical_merge_algorithm_min_rows_to_activate = 100000000;

-- The last row is the only one past its `DELETE` TTL, and the only one whose `clear_at` is in the future.
SYSTEM STOP TTL MERGES ttl_clear_index_after_delete_where;
INSERT INTO ttl_clear_index_after_delete_where
SELECT if(number = 19, '2000-01-01', '2100-01-01'), if(number = 19, '2100-01-01', '2000-01-01'), number = 19, number, number, number
FROM numbers(20);

SELECT name, data_compressed_bytes > 0 FROM system.data_skipping_indices
WHERE database = currentDatabase() AND table = 'ttl_clear_index_after_delete_where' ORDER BY name;

-- `OPTIMIZE FINAL` skips the merged part unless an index is still due to be cleared.
SYSTEM START TTL MERGES ttl_clear_index_after_delete_where;
OPTIMIZE TABLE ttl_clear_index_after_delete_where;
OPTIMIZE TABLE ttl_clear_index_after_delete_where FINAL SETTINGS optimize_skip_merged_partitions = 1;

SELECT count(), max(k) FROM ttl_clear_index_after_delete_where;
SELECT name, data_compressed_bytes > 0 FROM system.data_skipping_indices
WHERE database = currentDatabase() AND table = 'ttl_clear_index_after_delete_where' ORDER BY name;
-- Fails if the rewritten archive lost `idx_v`.
SELECT count() FROM ttl_clear_index_after_delete_where WHERE v = 7 SETTINGS max_rows_to_read = 2;
CHECK TABLE ttl_clear_index_after_delete_where SETTINGS check_query_single_value_result = 1;

SYSTEM FLUSH LOGS part_log;
SELECT count() FROM system.part_log
WHERE database = currentDatabase() AND table = 'ttl_clear_index_after_delete_where' AND event_type = 'MergeParts';

DROP TABLE ttl_clear_index_after_delete_where;
