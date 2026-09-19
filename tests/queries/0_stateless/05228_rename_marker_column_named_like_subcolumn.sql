-- The rename side of 05227_drop_marker_column_named_like_subcolumn. A column that a part records as
-- a missing-column marker is absent from `columns_substreams.txt`, so `RENAME COLUMN` of it took the
-- fallback that looks the column's serialization up in the part by name. For a marker named like a
-- subcolumn of another column (`a.size0` next to an `Array` column `a`) that name resolves to the
-- subcolumn's serialization, so the mutation renamed the array's offsets file away from the array
-- and every read of the array failed with a logical error. A rename has to keep the nested prefix
-- (`a.size0` to `a.z`), which is exactly the shape that still collides with the array's streams.

DROP TABLE IF EXISTS t_rename_marker_subcolumn;
CREATE TABLE t_rename_marker_subcolumn (a Array(UInt64)) ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0, skip_empty_columns_on_insert = 1,
         serialization_info_version = 'with_missing_columns';

ALTER TABLE t_rename_marker_subcolumn ADD COLUMN `a.size0` UInt64;
INSERT INTO t_rename_marker_subcolumn (a) VALUES ([10, 20, 30]);

SELECT 'before the rename', count(), sum(length(a)) FROM t_rename_marker_subcolumn;

ALTER TABLE t_rename_marker_subcolumn RENAME COLUMN `a.size0` TO `a.z` SETTINGS mutations_sync = 2;

SELECT 'after the rename', a, length(a), `a.z` FROM t_rename_marker_subcolumn;

-- The marker follows the rename, and dropping it afterwards touches no stream of the array either.
ALTER TABLE t_rename_marker_subcolumn DROP COLUMN `a.z` SETTINGS mutations_sync = 2;
SELECT 'after the drop', a, length(a) FROM t_rename_marker_subcolumn;

SELECT 'and the same for a Nested subcolumn name';
DROP TABLE IF EXISTS t_rename_marker_nested;
CREATE TABLE t_rename_marker_nested (n Nested(x UInt64, y UInt64)) ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0, skip_empty_columns_on_insert = 1,
         serialization_info_version = 'with_missing_columns';

ALTER TABLE t_rename_marker_nested ADD COLUMN `n.x.size0` UInt64;
INSERT INTO t_rename_marker_nested (`n.x`, `n.y`) VALUES ([1, 2], [3, 4]);

ALTER TABLE t_rename_marker_nested RENAME COLUMN `n.x.size0` TO `n.z` SETTINGS mutations_sync = 2;

SELECT `n.x`, `n.y`, `n.z` FROM t_rename_marker_nested;

SELECT 'a physically written column of that name is renamed as before';
DROP TABLE IF EXISTS t_rename_physical_subcolumn;
CREATE TABLE t_rename_physical_subcolumn (a Array(UInt64)) ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0, skip_empty_columns_on_insert = 1,
         serialization_info_version = 'with_missing_columns';

ALTER TABLE t_rename_physical_subcolumn ADD COLUMN `a.size0` UInt64;
INSERT INTO t_rename_physical_subcolumn (a, `a.size0`) VALUES ([10, 20, 30], 7);

ALTER TABLE t_rename_physical_subcolumn RENAME COLUMN `a.size0` TO `a.z` SETTINGS mutations_sync = 2;

SELECT a, length(a), `a.z` FROM t_rename_physical_subcolumn;

DROP TABLE t_rename_physical_subcolumn;
DROP TABLE t_rename_marker_nested;
DROP TABLE t_rename_marker_subcolumn;
