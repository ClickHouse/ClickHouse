-- A `TTL ... DELETE WHERE` must be evaluated against the values a merge produces, not only against
-- the values the source rows were written with. A merge that applies a patch part reads the patched
-- values, so a lightweight `UPDATE` can make the condition true in the output on any merge mode.
--
-- Split across several tests of the same number so no single one runs long on the slower CI
-- configurations: `_combined_values`, `_coalescing_and_graphite`, `_ttl_merges_stopped`,
-- `_background`.

SET session_timezone = 'UTC';

-- Patch parts are the other way a merge output holds a value no source row had, and they do it on
-- any merge mode: a lightweight `UPDATE` writes a patch part, the merge reads the patched value,
-- and
-- the part's `rows_where_ttl` was seeded from the base parts only. `Ordinary` here, so the mode
-- gate
-- above cannot be what saves it.
DROP TABLE IF EXISTS ttl_where_patched;

CREATE TABLE ttl_where_patched
(
    id UInt64,
    val UInt8,
    expiry DateTime
)
ENGINE = MergeTree
ORDER BY id
TTL expiry DELETE WHERE val = 0
SETTINGS min_bytes_for_wide_part = 0, apply_patches_on_merge = 1,
         enable_block_number_column = 1, enable_block_offset_column = 1;

SYSTEM STOP MERGES ttl_where_patched;

-- id 1: the patch makes it match the WHERE, and it is expired -> must be deleted.
INSERT INTO ttl_where_patched VALUES (1, 5, '2020-01-01 00:00:00');
-- id 2: the patch makes it match too, but it is not expired -> must survive.
INSERT INTO ttl_where_patched VALUES (2, 5, '2106-01-01 00:00:00');
-- id 3: expired, and the patch leaves it non-matching -> must survive.
INSERT INTO ttl_where_patched VALUES (3, 7, '2020-01-01 00:00:00');

UPDATE ttl_where_patched SET val = 0 WHERE id IN (1, 2)
SETTINGS enable_lightweight_update = 1, mutations_sync = 2;

SYSTEM START MERGES ttl_where_patched;
OPTIMIZE TABLE ttl_where_patched FINAL;

SELECT 'patched', id, val, expiry FROM ttl_where_patched ORDER BY id;

DROP TABLE ttl_where_patched;

-- The same on the vertical algorithm, which stays eligible for `Ordinary`: there the rows are
-- filtered before the merge by `TTLDeleteFilterTransform`, which skips a rows-WHERE TTL whose
-- source
-- info reports nothing expirable unless it is forced. The thresholds and part format are pinned so
-- the algorithm cannot silently fall back to Horizontal and test the path above twice - the last
-- assertion checks that it really was Vertical.
DROP TABLE IF EXISTS ttl_where_patched_vertical;

CREATE TABLE ttl_where_patched_vertical
(
    id UInt64,
    val UInt8,
    expiry DateTime,
    c1 UInt64,
    c2 String
)
ENGINE = MergeTree
ORDER BY id
TTL expiry DELETE WHERE val = 0
SETTINGS min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0, apply_patches_on_merge = 1,
         enable_block_number_column = 1, enable_block_offset_column = 1,
         enable_vertical_merge_algorithm = 1, vertical_merge_algorithm_min_rows_to_activate = 1,
         vertical_merge_algorithm_min_columns_to_activate = 1;

SYSTEM STOP MERGES ttl_where_patched_vertical;

INSERT INTO ttl_where_patched_vertical
    SELECT number, 5, toDateTime('2020-01-01 00:00:00'), number, 'x' FROM numbers(1000);
INSERT INTO ttl_where_patched_vertical
    SELECT number + 1000, 7, toDateTime('2020-01-01 00:00:00'), number, 'y' FROM numbers(1000);

UPDATE ttl_where_patched_vertical SET val = 0 WHERE id < 10
SETTINGS enable_lightweight_update = 1, mutations_sync = 2;

SYSTEM START MERGES ttl_where_patched_vertical;
OPTIMIZE TABLE ttl_where_patched_vertical FINAL;

SELECT 'patched vertical', count(), countIf(val = 0) FROM ttl_where_patched_vertical;

SYSTEM FLUSH LOGS part_log;
SELECT 'patched vertical algorithm', groupUniqArray(merge_algorithm) FROM system.part_log
WHERE event_date >= yesterday() AND database = currentDatabase()
  AND table = 'ttl_where_patched_vertical' AND event_type = 'MergeParts';

DROP TABLE ttl_where_patched_vertical;
