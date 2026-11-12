-- A column that a wide part holds only as a missing-column marker (`skip_empty_columns_on_insert`
-- with `serialization_info_version = 'with_missing_columns'`) has no streams of its own. When a
-- mutation counts the streams of the columns it reads and writes (to decide which files of the
-- source part are not hardlinked into the new part), the name of such a column must not be looked
-- up in the part's serializations: for a column named like a generated subcolumn of another column
-- (`a.size0` of an `Array` column `a`) that is the serialization of the array's offsets, and the
-- new part would be left without them. `ALTER TABLE ... CLEAR COLUMN` of the marker column, with a
-- skip index or a `MATERIALIZED` column that depends on it and has to be recomputed, exercises the
-- paths where the cleared column is read by the mutation while the part no longer stores it.

DROP TABLE IF EXISTS t_clear_marker_index;
CREATE TABLE t_clear_marker_index (a Array(UInt64)) ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0, skip_empty_columns_on_insert = 1,
    serialization_info_version = 'with_missing_columns';

ALTER TABLE t_clear_marker_index ADD COLUMN `a.size0` UInt64;
INSERT INTO t_clear_marker_index (a) VALUES ([10, 20, 30]), ([40]);
ALTER TABLE t_clear_marker_index ADD INDEX i `a.size0` TYPE minmax GRANULARITY 1;
ALTER TABLE t_clear_marker_index MATERIALIZE INDEX i SETTINGS mutations_sync = 2;

SELECT 'the array survives CLEAR COLUMN of the marker column with a dependent skip index';
ALTER TABLE t_clear_marker_index CLEAR COLUMN `a.size0` SETTINGS mutations_sync = 2;
SELECT a FROM t_clear_marker_index ORDER BY a;
SELECT sum(length(a)) FROM t_clear_marker_index;
CHECK TABLE t_clear_marker_index;

DROP TABLE IF EXISTS t_clear_marker_materialized;
CREATE TABLE t_clear_marker_materialized (a Array(UInt64)) ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0, skip_empty_columns_on_insert = 1,
    serialization_info_version = 'with_missing_columns';

ALTER TABLE t_clear_marker_materialized ADD COLUMN `a.size0` UInt64;
INSERT INTO t_clear_marker_materialized (a) VALUES ([10, 20, 30]), ([40]);
ALTER TABLE t_clear_marker_materialized ADD COLUMN m UInt64 MATERIALIZED `a.size0` + 1;
ALTER TABLE t_clear_marker_materialized MATERIALIZE COLUMN m SETTINGS mutations_sync = 2;
SELECT a, m FROM t_clear_marker_materialized ORDER BY a;

SELECT 'the array survives CLEAR COLUMN of the marker column with a dependent MATERIALIZED column';
ALTER TABLE t_clear_marker_materialized CLEAR COLUMN `a.size0` SETTINGS mutations_sync = 2;
SELECT a, m FROM t_clear_marker_materialized ORDER BY a;
SELECT sum(length(a)) FROM t_clear_marker_materialized;
CHECK TABLE t_clear_marker_materialized;

DROP TABLE IF EXISTS t_clear_marker_plain;
CREATE TABLE t_clear_marker_plain (a Array(UInt64)) ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0, skip_empty_columns_on_insert = 1,
    serialization_info_version = 'with_missing_columns';

ALTER TABLE t_clear_marker_plain ADD COLUMN `a.size0` UInt64;
INSERT INTO t_clear_marker_plain (a) VALUES ([10, 20, 30]), ([40]);

SELECT 'the array survives CLEAR COLUMN of the marker column without dependencies';
ALTER TABLE t_clear_marker_plain CLEAR COLUMN `a.size0` SETTINGS mutations_sync = 2;
SELECT a FROM t_clear_marker_plain ORDER BY a;
SELECT sum(length(a)) FROM t_clear_marker_plain;
CHECK TABLE t_clear_marker_plain;

DROP TABLE t_clear_marker_plain;
DROP TABLE t_clear_marker_materialized;
DROP TABLE t_clear_marker_index;
