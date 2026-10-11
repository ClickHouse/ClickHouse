-- Tags: no-replicated-database, no-shared-merge-tree
-- no-replicated-database, no-shared-merge-tree: `OPTIMIZE` without `FINAL` selects TTL merges only on plain `MergeTree`.

-- A `CLEAR INDEX` rule does not stop `ttl_only_drop_parts` from dropping a fully expired part without
-- reading it, and the drop is chosen over clearing the part's index.

DROP TABLE IF EXISTS ttl_clear_index_only_drop_parts;

CREATE TABLE ttl_clear_index_only_drop_parts
(
    d Date,
    k UInt64,
    v UInt64,
    INDEX idx v TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY k
TTL d + INTERVAL 1 DAY, d + INTERVAL 1 DAY CLEAR INDEX idx
SETTINGS
    ttl_only_drop_parts = 1,
    ttl_clear_index_merges = 1,
    max_number_of_merges_with_ttl_in_pool = 100,
    min_bytes_for_wide_part = 0,
    min_bytes_for_full_part_storage = 0;

INSERT INTO ttl_clear_index_only_drop_parts VALUES ('2000-01-01', 1, 1), ('2000-01-01', 2, 2);
OPTIMIZE TABLE ttl_clear_index_only_drop_parts;
-- Waits for a TTL merge that a background merge started.
OPTIMIZE TABLE ttl_clear_index_only_drop_parts FINAL;

SELECT count() FROM ttl_clear_index_only_drop_parts;
SYSTEM FLUSH LOGS part_log;
SELECT merge_reason, rows, read_rows FROM system.part_log
WHERE database = currentDatabase() AND table = 'ttl_clear_index_only_drop_parts' AND event_type = 'MergeParts'
ORDER BY event_time_microseconds LIMIT 1;

DROP TABLE ttl_clear_index_only_drop_parts;
