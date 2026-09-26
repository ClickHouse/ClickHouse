-- A column that a wide part holds only as a missing-column marker (`skip_empty_columns_on_insert`
-- with `serialization_info_version = 'with_missing_columns'`) has no streams of its own. Its files
-- were looked up through the part's serialization of that name, which - for a column named like a
-- generated subcolumn of another column (`a.size0` of an `Array` column `a`) - is the serialization
-- of that subcolumn. `ALTER TABLE ... DROP COLUMN` then removed the array's offsets stream from the
-- mutated part and every read of the array failed with an exception (issue #118255). `RENAME COLUMN`
-- enumerates the streams of the dropped name the same way, so it is covered here as well.

DROP TABLE IF EXISTS t_mutation_drops_offsets;
CREATE TABLE t_mutation_drops_offsets (a Array(UInt64)) ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0, skip_empty_columns_on_insert = 1,
    serialization_info_version = 'with_missing_columns';

ALTER TABLE t_mutation_drops_offsets ADD COLUMN `a.size0` UInt64;
INSERT INTO t_mutation_drops_offsets (a) VALUES ([10, 20, 30]), ([40]);

SELECT 'the array survives DROP COLUMN of the marker column';
ALTER TABLE t_mutation_drops_offsets DROP COLUMN `a.size0` SETTINGS mutations_sync = 2;
SELECT a FROM t_mutation_drops_offsets ORDER BY a;
SELECT sum(length(a)) FROM t_mutation_drops_offsets;

-- `RENAME COLUMN` of a marker column resolves the name the same way; the array keeps its offsets.
DROP TABLE IF EXISTS t_mutation_renames_offsets;
CREATE TABLE t_mutation_renames_offsets (a Array(UInt64)) ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0, skip_empty_columns_on_insert = 1,
    serialization_info_version = 'with_missing_columns';

ALTER TABLE t_mutation_renames_offsets ADD COLUMN `a.size0` UInt64;
INSERT INTO t_mutation_renames_offsets (a) VALUES ([10, 20, 30]), ([40]);

SELECT 'the array survives RENAME COLUMN of the marker column';
-- The new name stays inside the same `Nested` prefix: renaming a column out of it is rejected.
ALTER TABLE t_mutation_renames_offsets RENAME COLUMN `a.size0` TO `a.sz` SETTINGS mutations_sync = 2;
SELECT a, `a.sz` FROM t_mutation_renames_offsets ORDER BY a;
SELECT sum(length(a)) FROM t_mutation_renames_offsets;

-- A physically written column of the same name keeps its own files and loses them on the drop,
-- while the array keeps its offsets.
DROP TABLE IF EXISTS t_mutation_drops_physical;
CREATE TABLE t_mutation_drops_physical (a Array(UInt64)) ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0, skip_empty_columns_on_insert = 1,
    serialization_info_version = 'with_missing_columns';

ALTER TABLE t_mutation_drops_physical ADD COLUMN `a.size0` UInt64;
INSERT INTO t_mutation_drops_physical (a, `a.size0`) VALUES ([10, 20, 30], 7), ([40], 8);

SELECT 'the array survives DROP COLUMN of a physically written column';
ALTER TABLE t_mutation_drops_physical DROP COLUMN `a.size0` SETTINGS mutations_sync = 2;
SELECT a FROM t_mutation_drops_physical ORDER BY a;
SELECT sum(length(a)) FROM t_mutation_drops_physical;

DROP TABLE t_mutation_drops_physical;
DROP TABLE t_mutation_renames_offsets;
DROP TABLE t_mutation_drops_offsets;
