-- Tags: no-object-storage
-- An object-storage policy has no disk `default`, so the move TTL below could not be created there.

-- A vertical TTL merge evaluates every TTL family's expression against the merged block, so the
-- inputs of the move and recompression TTLs must be merged too, not gathered. Without that the
-- merge throws `NOT_FOUND_COLUMN_IN_BLOCK` and the parts never merge.
--
-- The rows TTL reads `d`, the recompression TTL reads `e` and the move TTL reads `f`: three
-- different columns, so none of them rides into the merged stream on another TTL's back.

SET optimize_throw_if_noop = 0;

DROP TABLE IF EXISTS t_ttl_vert_families;

CREATE TABLE t_ttl_vert_families
(
    id UInt64,
    d DateTime,
    e DateTime,
    f DateTime,
    c1 UInt64,
    c2 UInt64,
    c3 UInt64
)
ENGINE = MergeTree
ORDER BY id
TTL d + INTERVAL 1 DAY DELETE,
    e + INTERVAL 1 MONTH RECOMPRESS CODEC(ZSTD(1)),
    f + INTERVAL 2 MONTH TO DISK 'default'
SETTINGS
    min_bytes_for_wide_part = 0,
    min_bytes_for_full_part_storage = 0,
    enable_block_number_column = 0,
    enable_block_offset_column = 0,
    vertical_merge_algorithm_min_rows_to_activate = 1,
    vertical_merge_algorithm_min_columns_to_activate = 1,
    vertical_merge_optimize_ttl_delete = 1,
    max_bytes_to_merge_at_max_space_in_pool = 1,
    merge_with_ttl_timeout = 100000,
    ratio_of_defaults_for_sparse_serialization = 1.0;

-- Two parts, so the merge reads from the merging algorithm rather than short-circuiting a single
-- part. Even rows are expired by the rows TTL. `max_bytes_to_merge_at_max_space_in_pool = 1` keeps
-- the background pool off regular merges and the TTL blocker off TTL merges until both parts exist,
-- so the `OPTIMIZE` is the first merge logged.
SYSTEM STOP TTL MERGES t_ttl_vert_families;

INSERT INTO t_ttl_vert_families
SELECT number, if(number % 2, now() + INTERVAL 1 YEAR, '2000-01-01 00:00:00'), now(), now(), 1, 1, 1
FROM numbers(100);

INSERT INTO t_ttl_vert_families
SELECT number + 100, if(number % 2, now() + INTERVAL 1 YEAR, '2000-01-01 00:00:00'), now(), now(), 2, 2, 2
FROM numbers(100);

SYSTEM START TTL MERGES t_ttl_vert_families;

OPTIMIZE TABLE t_ttl_vert_families FINAL;

SELECT 'count', count() FROM t_ttl_vert_families;
SELECT 'expired_left', count() FROM t_ttl_vert_families WHERE d + INTERVAL 1 DAY <= now();
SELECT 'ids', min(id), max(id), countIf(id % 2 = 1) FROM t_ttl_vert_families;
SELECT 'cols', sum(c1), sum(c2), sum(c3) FROM t_ttl_vert_families;
SELECT 'parts', count() FROM system.parts
    WHERE database = currentDatabase() AND table = 't_ttl_vert_families' AND active;

SYSTEM FLUSH LOGS part_log;
-- Exclude `TTLDropMerge`: a part whose rows are all expired is dropped by a short-circuit that
-- always uses the `Horizontal` algorithm.
SELECT 'algo', merge_algorithm FROM system.part_log
    WHERE database = currentDatabase() AND table = 't_ttl_vert_families' AND event_type = 'MergeParts'
    AND merge_reason != 'TTLDropMerge'
    ORDER BY event_time_microseconds LIMIT 1;

DROP TABLE t_ttl_vert_families;
