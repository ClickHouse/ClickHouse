-- Coverage for src/Storages/System/StorageSystemProjectionParts.cpp and
-- src/Storages/System/StorageSystemProjectionPartsColumns.cpp.
-- Many columns (parent_uuid, parent_part_type, bytes_on_disk, marks_bytes, parent_marks,
-- parent_rows, parent_bytes_on_disk, parent_data_compressed_bytes,
-- parent_data_uncompressed_bytes, parent_marks_bytes, remove_time, refcount) in
-- system.projection_parts are never requested by CI tests; similarly for the
-- partition, part_type, parent_name, rows, bytes_on_disk, marks, modification_time
-- and related parent_* columns in system.projection_parts_columns.

DROP TABLE IF EXISTS t_proj_cov_05077;

-- val has a DEFAULT expression so projection_parts_columns.default_kind/default_expression
-- are non-empty, which hits the column.default_desc.expression branch in the fill function.
CREATE TABLE t_proj_cov_05077
(
    d    Date,
    key  UInt64,
    val  UInt64 DEFAULT 0,
    PROJECTION p_key (SELECT * ORDER BY key)
)
ENGINE = MergeTree()
ORDER BY d;

INSERT INTO t_proj_cov_05077 (d, key, val) SELECT toDate('2024-01-01'), number, number * 2 FROM numbers(100);
OPTIMIZE TABLE t_proj_cov_05077 FINAL;

-- system.projection_parts: exercise all previously-uncovered columns.
-- count(col) = count(*) is stable regardless of exact part count.
SELECT
    count(parent_uuid) = count(*)         AS has_parent_uuid,
    count(parent_part_type) = count(*)    AS has_parent_part_type,
    countIf(bytes_on_disk > 0)            AS has_bytes_on_disk,
    countIf(marks_bytes >= 0) = count(*)  AS has_marks_bytes,
    countIf(parent_marks > 0)             AS has_parent_marks,
    countIf(parent_rows > 0)              AS has_parent_rows,
    countIf(parent_bytes_on_disk > 0)     AS has_parent_bytes_on_disk,
    countIf(parent_data_compressed_bytes >= 0) = count(*) AS has_parent_compressed_bytes,
    countIf(parent_data_uncompressed_bytes >= 0) = count(*) AS has_parent_uncompressed_bytes,
    countIf(parent_marks_bytes >= 0) = count(*) AS has_parent_marks_bytes,
    countIf(remove_time = toDateTime(0))  AS active_no_remove_time,
    countIf(refcount >= 1) = count(*)     AS valid_refcount
FROM system.projection_parts
WHERE database = currentDatabase() AND table = 't_proj_cov_05077' AND active;

-- system.projection_parts_columns: exercise all previously-uncovered columns.
-- Note: projection_parts_columns.remove_time stores the raw time_t value without
-- the max→0 remapping used in projection_parts, so active parts show max time_t.
SELECT
    count(partition) = count(*)                           AS has_partition,
    count(part_type) = count(*)                           AS has_part_type,
    count(parent_name) = count(*)                         AS has_parent_name,
    count(parent_uuid) = count(*)                         AS has_parent_uuid,
    count(parent_part_type) = count(*)                    AS has_parent_part_type,
    countIf(marks > 0) = count(*)                         AS has_marks,
    countIf(rows > 0) = count(*)                          AS has_rows,
    countIf(bytes_on_disk > 0) = count(*)                 AS has_bytes_on_disk,
    countIf(data_uncompressed_bytes > 0) = count(*)       AS has_uncompressed_bytes,
    countIf(marks_bytes >= 0) = count(*)                  AS has_marks_bytes,
    countIf(parent_marks > 0) = count(*)                  AS has_parent_marks,
    countIf(parent_rows > 0) = count(*)                   AS has_parent_rows,
    countIf(parent_bytes_on_disk > 0) = count(*)          AS has_parent_bytes_on_disk,
    countIf(parent_data_compressed_bytes >= 0) = count(*) AS has_parent_compressed_bytes,
    countIf(parent_data_uncompressed_bytes >= 0) = count(*) AS has_parent_uncompressed_bytes,
    countIf(parent_marks_bytes >= 0) = count(*)           AS has_parent_marks_bytes,
    countIf(modification_time > toDateTime(0)) = count(*) AS has_modification_time,
    count(remove_time) = count(*)                         AS has_remove_time,
    countIf(refcount >= 1) = count(*)                     AS valid_refcount
FROM system.projection_parts_columns
WHERE database = currentDatabase() AND table = 't_proj_cov_05077' AND active;

-- Check that default_kind/default_expression are populated for the DEFAULT column (val).
-- This exercises the column.default_desc.expression branch in the fill function.
SELECT default_kind, default_expression
FROM system.projection_parts_columns
WHERE database = currentDatabase() AND table = 't_proj_cov_05077' AND active AND column = 'val' AND default_kind != ''
ORDER BY default_kind;

DROP TABLE t_proj_cov_05077;
