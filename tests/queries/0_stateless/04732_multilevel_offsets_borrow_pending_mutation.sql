SET allow_suspicious_low_cardinality_types = 1;

-- Reported shape: Array(Map(...)) has three ArraySizes levels (0 outer, 1 map-nested, 2 key array),
-- but only one directly nested Array dimension.
DROP TABLE IF EXISTS t_ml_map;
CREATE TABLE t_ml_map (c0 Int, m Array(Map(Array(DateTime64(6)), LowCardinality(Float64))))
ENGINE = MergeTree() ORDER BY tuple() SETTINGS min_bytes_for_wide_part = 1073741824, min_rows_for_wide_part = 1000000,
    map_serialization_version = 'basic', map_serialization_version_for_zero_level_parts = 'basic';
SYSTEM STOP MERGES t_ml_map;
INSERT INTO t_ml_map VALUES (1, [map([toDateTime64(1, 6)], 2.5)]);
ALTER TABLE t_ml_map CLEAR COLUMN m SETTINGS alter_sync = 0;
SELECT count() >= 1 FROM system.mutations WHERE database = currentDatabase() AND table = 't_ml_map' AND is_done = 0;
SELECT part_type FROM system.parts WHERE database = currentDatabase() AND table = 't_ml_map' AND active;
SELECT * FROM t_ml_map ORDER BY c0;
DROP TABLE t_ml_map;

-- Same defect without any Map: the Tuple hides a second offsets level.
DROP TABLE IF EXISTS t_ml_tuple;
CREATE TABLE t_ml_tuple (c0 Int, a Array(Tuple(k Array(UInt32), v String)))
ENGINE = MergeTree() ORDER BY tuple() SETTINGS min_bytes_for_wide_part = 1073741824, min_rows_for_wide_part = 1000000;
SYSTEM STOP MERGES t_ml_tuple;
INSERT INTO t_ml_tuple VALUES (1, [([7, 8], 'x')]);
ALTER TABLE t_ml_tuple CLEAR COLUMN a SETTINGS alter_sync = 0;
SELECT count() >= 1 FROM system.mutations WHERE database = currentDatabase() AND table = 't_ml_tuple' AND is_done = 0;
SELECT * FROM t_ml_tuple ORDER BY c0;
DROP TABLE t_ml_tuple;

-- A genuine pending DROP (clear = false) reaches the identical path.
DROP TABLE IF EXISTS t_ml_drop;
CREATE TABLE t_ml_drop (c0 Int, d Array(Tuple(k Array(UInt32), v String)))
ENGINE = MergeTree() ORDER BY tuple() SETTINGS min_bytes_for_wide_part = 1073741824, min_rows_for_wide_part = 1000000;
SYSTEM STOP MERGES t_ml_drop;
INSERT INTO t_ml_drop VALUES (1, [([7, 8], 'x')]);
ALTER TABLE t_ml_drop DROP COLUMN d SETTINGS alter_sync = 0;
ALTER TABLE t_ml_drop ADD COLUMN d Array(Tuple(k Array(UInt32), v String));
SELECT count() >= 1 FROM system.mutations WHERE database = currentDatabase() AND table = 't_ml_drop' AND is_done = 0;
SELECT * FROM t_ml_drop ORDER BY c0;
DROP TABLE t_ml_drop;

-- A subcolumn is deeper than its storage column, so the bound has to be taken in storage
-- coordinates. Substream marks present: the subcolumn tree is read directly.
DROP TABLE IF EXISTS t_ml_sub_on;
CREATE TABLE t_ml_sub_on (c0 Int, a Array(Tuple(k Array(UInt32), v String)))
ENGINE = MergeTree() ORDER BY tuple() SETTINGS min_bytes_for_wide_part = 1073741824, min_rows_for_wide_part = 1000000,
    write_marks_for_substreams_in_compact_parts = 1;
SYSTEM STOP MERGES t_ml_sub_on;
INSERT INTO t_ml_sub_on VALUES (1, [([7, 8], 'x'), ([9], 'y')]);
ALTER TABLE t_ml_sub_on CLEAR COLUMN a SETTINGS alter_sync = 0;
SELECT part_type FROM system.parts WHERE database = currentDatabase() AND table = 't_ml_sub_on' AND active;
SELECT count() > 0 FROM mergeTreeCodecBlockCounts(currentDatabase(), t_ml_sub_on);
SELECT c0, a.k, arrayMap(x -> length(x), a.k), a.v FROM t_ml_sub_on ORDER BY c0;
DROP TABLE t_ml_sub_on;

-- Same subcolumn read without substream marks: the whole storage column is deserialized and the
-- subcolumn extracted in memory, so needSkipStream is asked about the storage substream tree.
DROP TABLE IF EXISTS t_ml_sub_off;
CREATE TABLE t_ml_sub_off (c0 Int, a Array(Tuple(k Array(UInt32), v String)))
ENGINE = MergeTree() ORDER BY tuple() SETTINGS min_bytes_for_wide_part = 1073741824, min_rows_for_wide_part = 1000000,
    write_marks_for_substreams_in_compact_parts = 0;
SYSTEM STOP MERGES t_ml_sub_off;
INSERT INTO t_ml_sub_off VALUES (1, [([7, 8], 'x'), ([9], 'y')]);
ALTER TABLE t_ml_sub_off CLEAR COLUMN a SETTINGS alter_sync = 0;
SELECT part_type FROM system.parts WHERE database = currentDatabase() AND table = 't_ml_sub_off' AND active;
SELECT count() FROM mergeTreeCodecBlockCounts(currentDatabase(), t_ml_sub_off);
SELECT c0, a.k, arrayMap(x -> length(x), a.k), a.v FROM t_ml_sub_off ORDER BY c0;
DROP TABLE t_ml_sub_off;

-- Real Array(Array(T)) dimensions must still be borrowed: per-row lengths stay non-empty.
DROP TABLE IF EXISTS t_ml_dims;
CREATE TABLE t_ml_dims (c0 Int, n Array(Array(UInt8)))
ENGINE = MergeTree() ORDER BY tuple() SETTINGS min_bytes_for_wide_part = 1073741824, min_rows_for_wide_part = 1000000;
SYSTEM STOP MERGES t_ml_dims;
INSERT INTO t_ml_dims VALUES (1, [[1, 2], [3]]);
ALTER TABLE t_ml_dims CLEAR COLUMN n SETTINGS alter_sync = 0;
SELECT c0, n, length(n), arrayMap(x -> length(x), n) FROM t_ml_dims ORDER BY c0;
DROP TABLE t_ml_dims;

-- Single offsets level (the shape 04329 covers) is unaffected.
DROP TABLE IF EXISTS t_ml_single;
CREATE TABLE t_ml_single (c0 Int, c1 Array(Tuple(c2 Int)))
ENGINE = MergeTree() ORDER BY tuple() SETTINGS min_bytes_for_wide_part = 1073741824, min_rows_for_wide_part = 1000000;
SYSTEM STOP MERGES t_ml_single;
INSERT INTO t_ml_single VALUES (1, [(5)]);
ALTER TABLE t_ml_single CLEAR COLUMN c1 SETTINGS alter_sync = 0;
SELECT * FROM t_ml_single ORDER BY c0;
DROP TABLE t_ml_single;

-- Wide parts have no donor path: every shape above reads as an empty array.
DROP TABLE IF EXISTS t_ml_wide;
CREATE TABLE t_ml_wide (c0 Int, m Array(Map(Array(DateTime64(6)), LowCardinality(Float64))), n Array(Array(UInt8)))
ENGINE = MergeTree() ORDER BY tuple() SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
    map_serialization_version = 'basic', map_serialization_version_for_zero_level_parts = 'basic';
SYSTEM STOP MERGES t_ml_wide;
INSERT INTO t_ml_wide VALUES (1, [map([toDateTime64(1, 6)], 2.5)], [[1, 2], [3]]);
ALTER TABLE t_ml_wide CLEAR COLUMN m SETTINGS alter_sync = 0;
SELECT part_type FROM system.parts WHERE database = currentDatabase() AND table = 't_ml_wide' AND active;
SELECT * FROM t_ml_wide ORDER BY c0;
DROP TABLE t_ml_wide;
