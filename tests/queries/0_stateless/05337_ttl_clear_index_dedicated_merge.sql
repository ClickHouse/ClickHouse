-- Tags: no-replicated-database, no-shared-merge-tree
-- no-replicated-database, no-shared-merge-tree: `OPTIMIZE` without `FINAL` selects TTL merges only on plain `MergeTree`.

-- What a dedicated `TTLClearIndex` merge keeps and removes. Each table is filled before
-- `ttl_clear_index_merges` is enabled, and `OPTIMIZE` without `FINAL` then runs the merge. `TTLClearIndex` merges are
-- selected only while fewer than `max_number_of_merges_with_ttl_in_pool` TTL merges run on the whole
-- server, so the limit is raised.

SET enable_full_text_index = 1;

DROP TABLE IF EXISTS ttl_clear_index_packed;
DROP TABLE IF EXISTS ttl_clear_index_patch;
DROP TABLE IF EXISTS ttl_clear_index_retarget;
DROP TABLE IF EXISTS ttl_clear_index_rename;

-- `skp_idx.packed` holds the minmax indexes `idx_v` and `idx_w`, and only `idx_v` expires. The text index is never packed.
CREATE TABLE ttl_clear_index_packed
(
    d Date,
    k UInt64,
    v UInt64,
    w UInt64,
    s String,
    INDEX idx_v v TYPE minmax GRANULARITY 1,
    INDEX idx_w w TYPE minmax GRANULARITY 1,
    INDEX idx_s s TYPE text(tokenizer = splitByNonAlpha)
)
ENGINE = MergeTree
ORDER BY k
TTL d + INTERVAL 1 DAY CLEAR INDEX idx_v, d + INTERVAL 1 DAY CLEAR INDEX idx_s
SETTINGS
    ttl_clear_index_merges = 0,
    max_number_of_merges_with_ttl_in_pool = 100,
    packed_skip_index_max_bytes = '1Mi',
    index_granularity = 2,
    index_granularity_bytes = '10Mi',
    min_bytes_for_wide_part = 0,
    min_bytes_for_full_part_storage = 0;

INSERT INTO ttl_clear_index_packed SELECT '2000-01-01', number, number, number, 'token' || toString(number) FROM numbers(20);
ALTER TABLE ttl_clear_index_packed MODIFY SETTING ttl_clear_index_merges = 1;
OPTIMIZE TABLE ttl_clear_index_packed;
OPTIMIZE TABLE ttl_clear_index_packed FINAL SETTINGS optimize_skip_merged_partitions = 1;

SELECT name, data_compressed_bytes > 0 FROM system.data_skipping_indices
WHERE database = currentDatabase() AND table = 'ttl_clear_index_packed' ORDER BY name;
-- Fails if the rewritten archive lost `idx_w`.
SELECT count() FROM ttl_clear_index_packed WHERE w = 7 SETTINGS max_rows_to_read = 2;
SELECT count() FROM ttl_clear_index_packed WHERE hasToken(s, 'token7');
CHECK TABLE ttl_clear_index_packed SETTINGS check_query_single_value_result = 1;

-- A `TTLClearIndex` merge leaves pending patch parts out, and they still apply to the result at read time.
CREATE TABLE ttl_clear_index_patch
(
    d Date,
    k UInt64,
    v UInt64,
    INDEX idx v TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY k
TTL d + INTERVAL 1 DAY CLEAR INDEX idx
SETTINGS
    ttl_clear_index_merges = 0,
    max_number_of_merges_with_ttl_in_pool = 100,
    apply_patches_on_merge = 1,
    enable_block_number_column = 1,
    enable_block_offset_column = 1,
    min_bytes_for_wide_part = 0,
    min_bytes_for_full_part_storage = 0;

INSERT INTO ttl_clear_index_patch VALUES ('2000-01-01', 1, 1), ('2000-01-01', 2, 2);
UPDATE ttl_clear_index_patch SET v = 100 WHERE k = 1;
ALTER TABLE ttl_clear_index_patch MODIFY SETTING ttl_clear_index_merges = 1;
OPTIMIZE TABLE ttl_clear_index_patch;
OPTIMIZE TABLE ttl_clear_index_patch FINAL SETTINGS optimize_skip_merged_partitions = 1;

SELECT startsWith(name, 'patch-'), level FROM system.parts
WHERE database = currentDatabase() AND table = 'ttl_clear_index_patch' AND active ORDER BY name;
SELECT data_compressed_bytes FROM system.data_skipping_indices
WHERE database = currentDatabase() AND table = 'ttl_clear_index_patch';
SELECT groupArray(v) FROM (SELECT v FROM ttl_clear_index_patch ORDER BY k);
SELECT groupArray(v) FROM (SELECT v FROM ttl_clear_index_patch ORDER BY k) SETTINGS apply_patch_parts = 0;

-- `MODIFY TTL` points the same TTL expression at `idx_b`. The TTL info is keyed by the expression,
-- so the cleared part is due again, now for `idx_b`.
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
SETTINGS ttl_clear_index_merges = 0, max_number_of_merges_with_ttl_in_pool = 100, packed_skip_index_max_bytes = 0, min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0;

INSERT INTO ttl_clear_index_retarget VALUES ('2000-01-01', 1, 1), ('2000-01-01', 2, 2);
ALTER TABLE ttl_clear_index_retarget MODIFY SETTING ttl_clear_index_merges = 1;
OPTIMIZE TABLE ttl_clear_index_retarget;
OPTIMIZE TABLE ttl_clear_index_retarget FINAL SETTINGS optimize_skip_merged_partitions = 1;
SELECT name, data_compressed_bytes > 0 FROM system.data_skipping_indices
WHERE database = currentDatabase() AND table = 'ttl_clear_index_retarget' ORDER BY name;
ALTER TABLE ttl_clear_index_retarget MODIFY TTL d + INTERVAL 1 DAY CLEAR INDEX idx_b SETTINGS materialize_ttl_after_modify = 0;
OPTIMIZE TABLE ttl_clear_index_retarget;
OPTIMIZE TABLE ttl_clear_index_retarget FINAL SETTINGS optimize_skip_merged_partitions = 1;
SELECT name, data_compressed_bytes > 0 FROM system.data_skipping_indices
WHERE database = currentDatabase() AND table = 'ttl_clear_index_retarget' ORDER BY name;

-- A `TTLClearIndex` merge does not wait for a pending `RENAME COLUMN`. The result keeps the source data version, so
-- the rename still applies to it.
CREATE TABLE ttl_clear_index_rename
(
    d Date,
    k UInt64,
    v UInt64,
    INDEX idx v TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY k
TTL d + INTERVAL 1 DAY CLEAR INDEX idx
SETTINGS ttl_clear_index_merges = 1, max_number_of_merges_with_ttl_in_pool = 100, min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0;

SYSTEM STOP MERGES ttl_clear_index_rename;
INSERT INTO ttl_clear_index_rename VALUES ('2000-01-01', 1, 1), ('2000-01-01', 2, 2);
ALTER TABLE ttl_clear_index_rename RENAME COLUMN v TO renamed SETTINGS alter_sync = 0, mutations_sync = 0;
-- Background selection tries merges before mutations, so the `TTLClearIndex` merge runs before the rename mutation.
SYSTEM START MERGES ttl_clear_index_rename;
OPTIMIZE TABLE ttl_clear_index_rename;
OPTIMIZE TABLE ttl_clear_index_rename FINAL SETTINGS optimize_skip_merged_partitions = 1;
SELECT sum(renamed), count() FROM ttl_clear_index_rename;

SYSTEM FLUSH LOGS part_log;
-- The rename source still had data version 1 when it was cleared.
SELECT table, part_name FROM system.part_log
WHERE database = currentDatabase() AND table = 'ttl_clear_index_rename' AND merge_reason = 'TTLClearIndexMerge' AND event_type = 'MergeParts';
SELECT table, groupArray(merge_reason)[-1], countIf(merge_reason = 'TTLClearIndexMerge')
FROM (
    SELECT table, merge_reason FROM system.part_log
    WHERE database = currentDatabase() AND event_type = 'MergeParts' ORDER BY event_time_microseconds
)
GROUP BY table ORDER BY table;

DROP TABLE ttl_clear_index_packed;
DROP TABLE ttl_clear_index_patch;
DROP TABLE ttl_clear_index_retarget;
DROP TABLE ttl_clear_index_rename;
