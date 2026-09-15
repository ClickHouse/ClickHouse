-- A column that a part records as a missing-column marker has no streams of its own, and its
-- serialization used to be looked up in the part by name. For a marker named like a subcolumn of
-- another column (`a.size0` next to an `Array` column `a`) that name resolves to the subcolumn's
-- serialization, so `DROP COLUMN` of the marker removed the array's offsets file from the mutated
-- part and every read of the array failed with a logical error.

DROP TABLE IF EXISTS t_drop_marker_subcolumn;
CREATE TABLE t_drop_marker_subcolumn (a Array(UInt64)) ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0, skip_empty_columns_on_insert = 1,
         serialization_info_version = 'with_missing_columns';

ALTER TABLE t_drop_marker_subcolumn ADD COLUMN `a.size0` UInt64;
INSERT INTO t_drop_marker_subcolumn (a) VALUES ([10, 20, 30]);

SELECT 'before the drop', count(), sum(length(a)) FROM t_drop_marker_subcolumn;

ALTER TABLE t_drop_marker_subcolumn DROP COLUMN `a.size0` SETTINGS mutations_sync = 2;

SELECT 'after the drop', a, length(a) FROM t_drop_marker_subcolumn;

SELECT 'and the same for a Nested subcolumn name';
DROP TABLE IF EXISTS t_drop_marker_nested;
CREATE TABLE t_drop_marker_nested (n Nested(x UInt64, y UInt64)) ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0, skip_empty_columns_on_insert = 1,
         serialization_info_version = 'with_missing_columns';

ALTER TABLE t_drop_marker_nested ADD COLUMN `n.x.size0` UInt64;
INSERT INTO t_drop_marker_nested (`n.x`, `n.y`) VALUES ([1, 2], [3, 4]);

ALTER TABLE t_drop_marker_nested DROP COLUMN `n.x.size0` SETTINGS mutations_sync = 2;

SELECT `n.x`, `n.y` FROM t_drop_marker_nested;

SELECT 'a physically written column of that name is dropped as before';
DROP TABLE IF EXISTS t_drop_physical_subcolumn;
CREATE TABLE t_drop_physical_subcolumn (a Array(UInt64)) ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0, skip_empty_columns_on_insert = 1,
         serialization_info_version = 'with_missing_columns';

ALTER TABLE t_drop_physical_subcolumn ADD COLUMN `a.size0` UInt64;
INSERT INTO t_drop_physical_subcolumn (a, `a.size0`) VALUES ([10, 20, 30], 7);

ALTER TABLE t_drop_physical_subcolumn DROP COLUMN `a.size0` SETTINGS mutations_sync = 2;

SELECT a, length(a) FROM t_drop_physical_subcolumn;

DROP TABLE t_drop_physical_subcolumn;
DROP TABLE t_drop_marker_nested;
DROP TABLE t_drop_marker_subcolumn;
