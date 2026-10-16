-- Tags: no-replicated-database, no-shared-merge-tree
-- no-replicated-database, no-shared-merge-tree: `OPTIMIZE` without `FINAL` selects TTL merges only on plain `MergeTree`.

-- Which parts a dedicated `TTLClearIndex` merge may take. Each table holds one part whose index has
-- expired. Each table is filled before `ttl_clear_index_merges` is enabled, and `OPTIMIZE` without
-- `FINAL` then selects any TTL merge that is due. `TTLClearIndex` merges are selected only while fewer than
-- `max_number_of_merges_with_ttl_in_pool` TTL merges run on the whole server, so the limit is raised.

DROP TABLE IF EXISTS ttl_clear_index_link;
DROP TABLE IF EXISTS ttl_clear_index_copy;
DROP TABLE IF EXISTS ttl_clear_index_packed;
DROP TABLE IF EXISTS ttl_clear_index_uuid;
DROP TABLE IF EXISTS ttl_clear_index_no_files;
DROP TABLE IF EXISTS ttl_clear_index_replacing;
DROP TABLE IF EXISTS ttl_clear_index_off;

-- The size limit stops regular merges. A `TTLClearIndex` merge hardlinks the files and ignores it. A
-- table that copies files instead of hardlinking them gets no `TTLClearIndex` merges.
CREATE TABLE ttl_clear_index_link
(
    d Date,
    k UInt64,
    v String,
    INDEX idx v TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY k
TTL d + INTERVAL 1 DAY CLEAR INDEX idx
SETTINGS
    ttl_clear_index_merges = 0,
    max_number_of_merges_with_ttl_in_pool = 100,
    always_use_copy_instead_of_hardlinks = 0,
    max_bytes_to_merge_at_min_space_in_pool = 1,
    max_bytes_to_merge_at_max_space_in_pool = 1,
    min_bytes_for_wide_part = 0,
    min_bytes_for_full_part_storage = 0;

CREATE TABLE ttl_clear_index_copy AS ttl_clear_index_link;
ALTER TABLE ttl_clear_index_copy MODIFY SETTING always_use_copy_instead_of_hardlinks = 1;

CREATE TABLE ttl_clear_index_packed AS ttl_clear_index_link;
ALTER TABLE ttl_clear_index_packed MODIFY SETTING min_bytes_for_full_part_storage = '1Gi', min_rows_for_full_part_storage = 1000000;

CREATE TABLE ttl_clear_index_uuid AS ttl_clear_index_link;
ALTER TABLE ttl_clear_index_uuid MODIFY SETTING assign_part_uuids = 1;

CREATE TABLE ttl_clear_index_no_files AS ttl_clear_index_link;

CREATE TABLE ttl_clear_index_replacing
(
    d Date,
    k UInt64,
    v String,
    INDEX idx v TYPE minmax GRANULARITY 1
)
ENGINE = ReplacingMergeTree
ORDER BY k
TTL d + INTERVAL 1 DAY CLEAR INDEX idx
SETTINGS ttl_clear_index_merges = 0, max_number_of_merges_with_ttl_in_pool = 100, min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0;

INSERT INTO ttl_clear_index_link VALUES ('2000-01-01', 1, 'a'), ('2000-01-01', 2, 'b');
INSERT INTO ttl_clear_index_copy VALUES ('2000-01-01', 1, 'a'), ('2000-01-01', 2, 'b');
INSERT INTO ttl_clear_index_packed VALUES ('2000-01-01', 1, 'a'), ('2000-01-01', 2, 'b');
INSERT INTO ttl_clear_index_uuid VALUES ('2000-01-01', 1, 'a'), ('2000-01-01', 2, 'b');
INSERT INTO ttl_clear_index_no_files SETTINGS materialize_skip_indexes_on_insert = 0 VALUES ('2000-01-01', 1, 'a'), ('2000-01-01', 2, 'b');
INSERT INTO ttl_clear_index_replacing SETTINGS optimize_on_insert = 0 VALUES ('2000-01-01', 1, 'a'), ('2000-01-01', 1, 'b'), ('2000-01-01', 2, 'c');

-- `OPTIMIZE FINAL` with `optimize_skip_merged_partitions` waits for a `TTLClearIndex` merge that a
-- background merge started, and then skips the new part. It runs after each `OPTIMIZE` that should start a
-- `TTLClearIndex` merge.
ALTER TABLE ttl_clear_index_link MODIFY SETTING ttl_clear_index_merges = 1;
OPTIMIZE TABLE ttl_clear_index_link;
OPTIMIZE TABLE ttl_clear_index_link FINAL SETTINGS optimize_skip_merged_partitions = 1;

ALTER TABLE ttl_clear_index_copy MODIFY SETTING ttl_clear_index_merges = 1;
OPTIMIZE TABLE ttl_clear_index_copy;
ALTER TABLE ttl_clear_index_packed MODIFY SETTING ttl_clear_index_merges = 1;
OPTIMIZE TABLE ttl_clear_index_packed;
ALTER TABLE ttl_clear_index_uuid MODIFY SETTING ttl_clear_index_merges = 1;
OPTIMIZE TABLE ttl_clear_index_uuid;
ALTER TABLE ttl_clear_index_no_files MODIFY SETTING ttl_clear_index_merges = 1;
OPTIMIZE TABLE ttl_clear_index_no_files;
-- A level-0 part of `ReplacingMergeTree` may hold duplicate keys, and a `TTLClearIndex` merge would raise its level.
ALTER TABLE ttl_clear_index_replacing MODIFY SETTING ttl_clear_index_merges = 1;
OPTIMIZE TABLE ttl_clear_index_replacing;

SELECT table, part_storage_type, level, rows
FROM system.parts WHERE database = currentDatabase() AND active ORDER BY table;
SELECT table, data_compressed_bytes > 0
FROM system.data_skipping_indices WHERE database = currentDatabase() ORDER BY table;

-- A regular merge collapses the rows and clears the index. Once the part is above level 0, a `TTLClearIndex`
-- merge may take it.
OPTIMIZE TABLE ttl_clear_index_replacing FINAL;
SELECT k, v FROM ttl_clear_index_replacing ORDER BY k;
ALTER TABLE ttl_clear_index_replacing MATERIALIZE INDEX idx SETTINGS mutations_sync = 2;
OPTIMIZE TABLE ttl_clear_index_replacing;
OPTIMIZE TABLE ttl_clear_index_replacing FINAL SETTINGS optimize_skip_merged_partitions = 1;

SELECT table, data_compressed_bytes > 0
FROM system.data_skipping_indices WHERE database = currentDatabase() ORDER BY table;

SYSTEM FLUSH LOGS part_log;
SELECT table, groupArray(merge_reason)
FROM (
    SELECT table, merge_reason FROM system.part_log
    WHERE database = currentDatabase() AND event_type = 'MergeParts'
    ORDER BY event_time_microseconds
)
GROUP BY table ORDER BY table;

-- With `ttl_clear_index_merges = 0`, `OPTIMIZE FINAL` with `optimize_skip_merged_partitions` still rewrites a
-- merged part whose index has expired, and skips the part once the index files are gone.
CREATE TABLE ttl_clear_index_off
(
    d Date,
    k UInt64,
    v String,
    INDEX idx v TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY k
TTL d + INTERVAL 1 DAY CLEAR INDEX idx
SETTINGS ttl_clear_index_merges = 0, min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0;

INSERT INTO ttl_clear_index_off VALUES ('2000-01-01', 1, 'a'), ('2000-01-01', 2, 'b');
OPTIMIZE TABLE ttl_clear_index_off FINAL;
ALTER TABLE ttl_clear_index_off MATERIALIZE INDEX idx SETTINGS mutations_sync = 2;
SELECT 'materialized', level, secondary_indices_compressed_bytes > 0
FROM system.parts WHERE database = currentDatabase() AND table = 'ttl_clear_index_off' AND active;
OPTIMIZE TABLE ttl_clear_index_off;
SELECT 'no TTLClearIndex merge', level, secondary_indices_compressed_bytes > 0
FROM system.parts WHERE database = currentDatabase() AND table = 'ttl_clear_index_off' AND active;
OPTIMIZE TABLE ttl_clear_index_off FINAL SETTINGS optimize_skip_merged_partitions = 1;
SELECT 'rewritten', level, secondary_indices_compressed_bytes > 0
FROM system.parts WHERE database = currentDatabase() AND table = 'ttl_clear_index_off' AND active;
OPTIMIZE TABLE ttl_clear_index_off FINAL SETTINGS optimize_skip_merged_partitions = 1;
SELECT 'skipped', level, secondary_indices_compressed_bytes > 0
FROM system.parts WHERE database = currentDatabase() AND table = 'ttl_clear_index_off' AND active;

DROP TABLE ttl_clear_index_link;
DROP TABLE ttl_clear_index_copy;
DROP TABLE ttl_clear_index_packed;
DROP TABLE ttl_clear_index_uuid;
DROP TABLE ttl_clear_index_no_files;
DROP TABLE ttl_clear_index_replacing;
DROP TABLE ttl_clear_index_off;
