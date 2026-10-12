SET optimize_trivial_count_query = 0;
SET optimize_use_projections = 1;
SET optimize_use_implicit_projections = 1;
SET apply_deleted_mask = 1;
SET lightweight_deletes_sync = 2;
SET mutations_sync = 2;
SET lightweight_delete_mode = 'alter_update';

DROP TABLE IF EXISTS lwd_count_wide;
DROP TABLE IF EXISTS lwd_count_compact;
DROP TABLE IF EXISTS lwd_count_old;
DROP TABLE IF EXISTS lwd_count_mutation;

SELECT 'wide parts';
CREATE TABLE lwd_count_wide (p UInt8, id UInt64, value UInt64)
ENGINE = MergeTree PARTITION BY p ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
    index_granularity = 32, index_granularity_bytes = 0,
    ratio_of_defaults_for_sparse_serialization = 0,
    exclude_deleted_rows_for_part_size_in_merge = 1, load_existing_rows_count_for_old_parts = 1;
-- Three granules suffice to check exact-range counting without expensive per-granule reads on reload.
INSERT INTO lwd_count_wide SELECT 0, number, number FROM numbers(80);
DELETE FROM lwd_count_wide WHERE id IN (0, 40, 79);

-- An exact live count enables the projection, but extrema must still exclude deleted rows.
SELECT count() FROM lwd_count_wide SETTINGS force_optimize_projection_name = '_minmax_count_projection';
SELECT min(id) FROM lwd_count_wide;
SELECT min(id), max(id) FROM lwd_count_wide;
SELECT min(id), max(id), count() FROM lwd_count_wide;
SELECT count() FROM lwd_count_wide SETTINGS apply_deleted_mask = 0, force_optimize_projection_name = '_minmax_count_projection';

-- Primary-key filters must not use physical granule counts.
SELECT count() FROM lwd_count_wide WHERE id < 32;
SELECT count() FROM lwd_count_wide WHERE id >= 32 AND id < 64;
SELECT p, count() FROM lwd_count_wide GROUP BY p ORDER BY p;

-- A partial-column mutation preserves an unchanged mask and its exact count.
ALTER TABLE lwd_count_wide UPDATE value = value + 1 WHERE id = 1;
SELECT count() FROM lwd_count_wide SETTINGS force_optimize_projection_name = '_minmax_count_projection';
DETACH TABLE lwd_count_wide;
ATTACH TABLE lwd_count_wide;
SELECT count() FROM lwd_count_wide SETTINGS force_optimize_projection_name = '_minmax_count_projection';

-- A physically nonempty part with no visible rows must not create a group.
DELETE FROM lwd_count_wide WHERE 1;
DETACH TABLE lwd_count_wide;
ATTACH TABLE lwd_count_wide;
SELECT count() FROM lwd_count_wide;
SELECT count() FROM lwd_count_wide SETTINGS empty_result_for_aggregation_by_empty_set = 1;
SELECT count() FROM lwd_count_wide GROUP BY toUInt8(0);
SELECT count() FROM (SELECT _part, count() FROM lwd_count_wide GROUP BY _part);
SELECT count() FROM (SELECT p, count() FROM lwd_count_wide GROUP BY p);

SELECT 'compact parts';
CREATE TABLE lwd_count_compact (id UInt64, value UInt64)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 1073741824, min_rows_for_wide_part = 0,
    exclude_deleted_rows_for_part_size_in_merge = 1, load_existing_rows_count_for_old_parts = 1;
INSERT INTO lwd_count_compact SELECT number, number FROM numbers(100);
DELETE FROM lwd_count_compact WHERE id IN (0, 50, 99);
SELECT count() FROM lwd_count_compact SETTINGS force_optimize_projection_name = '_minmax_count_projection';
SELECT min(id), max(id) FROM lwd_count_compact;
SELECT count() FROM lwd_count_compact SETTINGS apply_deleted_mask = 0, force_optimize_projection_name = '_minmax_count_projection';

-- A new deletion without counting must invalidate the previously known count.
ALTER TABLE lwd_count_compact MODIFY SETTING exclude_deleted_rows_for_part_size_in_merge = 0;
DELETE FROM lwd_count_compact WHERE id = 1;
ALTER TABLE lwd_count_compact MODIFY SETTING exclude_deleted_rows_for_part_size_in_merge = 1;
SELECT count() FROM lwd_count_compact;
ALTER TABLE lwd_count_compact UPDATE value = value + 1 WHERE id = 2;
SELECT count() FROM lwd_count_compact;
DETACH TABLE lwd_count_compact;
ATTACH TABLE lwd_count_compact;
SELECT count() FROM lwd_count_compact SETTINGS force_optimize_projection_name = '_minmax_count_projection';
DELETE FROM lwd_count_compact WHERE 1;
SELECT count() FROM (SELECT _part, count() FROM lwd_count_compact GROUP BY _part);

SELECT 'settings changes and old parts';
CREATE TABLE lwd_count_old (p UInt8, id UInt64)
ENGINE = MergeTree PARTITION BY p ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
    exclude_deleted_rows_for_part_size_in_merge = 0, load_existing_rows_count_for_old_parts = 0;
INSERT INTO lwd_count_old SELECT 0, number FROM numbers(100);
DELETE FROM lwd_count_old WHERE id = 0;
ALTER TABLE lwd_count_old MODIFY SETTING exclude_deleted_rows_for_part_size_in_merge = 1, load_existing_rows_count_for_old_parts = 1;
SELECT count() FROM lwd_count_old;
SELECT count() FROM lwd_count_old SETTINGS apply_deleted_mask = 0, force_optimize_projection_name = '_minmax_count_projection';

-- Loading without counting must also leave the count unknown after settings change.
ALTER TABLE lwd_count_old MODIFY SETTING load_existing_rows_count_for_old_parts = 0;
DETACH TABLE lwd_count_old;
ATTACH TABLE lwd_count_old;
ALTER TABLE lwd_count_old MODIFY SETTING load_existing_rows_count_for_old_parts = 1;
SELECT count() FROM lwd_count_old;

-- Do not mix a physical count from an unknown part with a live count from another part.
INSERT INTO lwd_count_old SELECT 1, number FROM numbers(100);
DELETE FROM lwd_count_old WHERE p = 1 AND id = 0;
SELECT count() FROM lwd_count_old;
SELECT count() FROM lwd_count_old WHERE p = 1 SETTINGS force_optimize_projection_name = '_minmax_count_projection';
DETACH TABLE lwd_count_old;
ATTACH TABLE lwd_count_old;
SELECT count() FROM lwd_count_old SETTINGS force_optimize_projection_name = '_minmax_count_projection';

SELECT 'row-removing mutations';
CREATE TABLE lwd_count_mutation (id UInt64) ENGINE = MergeTree ORDER BY id
SETTINGS exclude_deleted_rows_for_part_size_in_merge = 1, load_existing_rows_count_for_old_parts = 1;
INSERT INTO lwd_count_mutation SELECT number FROM numbers(100);
ALTER TABLE lwd_count_mutation DELETE WHERE id = 0;
SELECT count() FROM lwd_count_mutation SETTINGS force_optimize_projection_name = '_minmax_count_projection';
DELETE FROM lwd_count_mutation WHERE id = 1;
ALTER TABLE lwd_count_mutation DELETE WHERE id = 2;
SELECT count() FROM lwd_count_mutation;
SELECT count() FROM lwd_count_mutation SETTINGS optimize_use_implicit_projections = 0;

DROP TABLE lwd_count_wide;
DROP TABLE lwd_count_compact;
DROP TABLE lwd_count_old;
DROP TABLE lwd_count_mutation;
