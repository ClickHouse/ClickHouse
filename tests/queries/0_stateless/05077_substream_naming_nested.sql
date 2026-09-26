-- Nested is the shape where the two naming schemes genuinely differ on disk. A flattened group is
-- stored as separate physical columns that share one offsets stream, and the reader resolves stream
-- names from those physical columns; every check below would break if it resolved them from a
-- collected Nested view instead.

DROP TABLE IF EXISTS t_flat_basic;
DROP TABLE IF EXISTS t_flat_ns;
DROP TABLE IF EXISTS t_unflat_basic;
DROP TABLE IF EXISTS t_unflat_ns;

-- CI randomizes hashing and sparse serialization, and both change the file set, so they are pinned
-- wherever file names are asserted below.
CREATE TABLE t_flat_basic (n Nested(b UInt64, c UInt64)) ENGINE = MergeTree ORDER BY tuple()
    SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, substream_naming_version = 'basic',
             replace_long_file_name_to_hash = 0, ratio_of_defaults_for_sparse_serialization = 1;
CREATE TABLE t_flat_ns (n Nested(b UInt64, c UInt64)) ENGINE = MergeTree ORDER BY tuple()
    SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, substream_naming_version = 'namespaced',
             replace_long_file_name_to_hash = 0, ratio_of_defaults_for_sparse_serialization = 1;

INSERT INTO t_flat_basic VALUES ([1,2],[3,4]);
INSERT INTO t_flat_ns VALUES ([1,2],[3,4]);

SELECT 'flattened, one column at a time';
SELECT n.b FROM t_flat_basic;
SELECT n.b FROM t_flat_ns;
SELECT n.c FROM t_flat_basic;
SELECT n.c FROM t_flat_ns;

-- Both columns in one query read the shared offsets stream once. Without that sharing the second
-- column advances the stream past the first column's data, which returns wrong values rather than
-- raising an error.
SELECT 'flattened, both columns together';
SELECT n.b, n.c FROM t_flat_basic;
SELECT n.b, n.c FROM t_flat_ns;

SELECT 'flattened, offsets subcolumn';
SELECT n.b.size0, n.c.size0 FROM t_flat_basic;
SELECT n.b.size0, n.c.size0 FROM t_flat_ns;

-- One data file per column plus a single offsets file named after the group, not after either
-- column. Under 'namespaced' the offsets file loses its level number.
SELECT 'flattened layout';
SELECT arraySort(groupArrayDistinct(f)) FROM
    (SELECT arrayJoin(filenames) AS f FROM system.parts_columns
     WHERE database = currentDatabase() AND table = 't_flat_basic' AND active);
SELECT arraySort(groupArrayDistinct(f)) FROM
    (SELECT arrayJoin(filenames) AS f FROM system.parts_columns
     WHERE database = currentDatabase() AND table = 't_flat_ns' AND active);

SET flatten_nested = 0;
CREATE TABLE t_unflat_basic (n Nested(b UInt64, c UInt64)) ENGINE = MergeTree ORDER BY tuple()
    SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, substream_naming_version = 'basic';
CREATE TABLE t_unflat_ns (n Nested(b UInt64, c UInt64)) ENGINE = MergeTree ORDER BY tuple()
    SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, substream_naming_version = 'namespaced';
INSERT INTO t_unflat_basic VALUES ([(1,3),(2,4)]);
INSERT INTO t_unflat_ns VALUES ([(1,3),(2,4)]);
SET flatten_nested = 1;

SELECT 'unflattened';
SELECT n.b, n.c FROM t_unflat_basic;
SELECT n.b, n.c FROM t_unflat_ns;

DROP TABLE t_flat_basic;
DROP TABLE t_flat_ns;
DROP TABLE t_unflat_basic;
DROP TABLE t_unflat_ns;

-- Deeper element types: only the group's top level offsets are shared, so an array or a tuple inside
-- an element keeps its own streams.
DROP TABLE IF EXISTS t_deep;
CREATE TABLE t_deep (n Nested(b Array(UInt64), c Tuple(x UInt64), d Nullable(String)))
    ENGINE = MergeTree ORDER BY tuple()
    SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, substream_naming_version = 'namespaced';
INSERT INTO t_deep VALUES ([[1,2],[3]], [(4),(5)], ['ab',NULL]);

SELECT 'deep elements';
SELECT n.b, n.c, n.d FROM t_deep;
SELECT n.b.size0, n.b.size1 FROM t_deep;
SELECT n.c.x FROM t_deep;
SELECT n.d.null FROM t_deep;
DROP TABLE t_deep;

-- A column added to the group after the part was written must be filled with arrays as long as its
-- surviving siblings. Reading resolves physical columns while missing column filling still uses the
-- collected Nested view, so this checks the two stayed consistent.
DROP TABLE IF EXISTS t_missing;
CREATE TABLE t_missing (n Nested(a UInt64, b UInt64)) ENGINE = MergeTree ORDER BY tuple()
    SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, substream_naming_version = 'namespaced';
INSERT INTO t_missing VALUES ([1,2,3],[4,5,6]);
ALTER TABLE t_missing ADD COLUMN `n.c` Array(UInt64);

SELECT 'missing column of a group';
SELECT n.a, n.c FROM t_missing;
SELECT length(n.a) = length(n.c) FROM t_missing;
DROP TABLE t_missing;

-- With the sharing disabled each column keeps its own offsets file, so the cache must not make them
-- meet. The lengths differ so that a mix-up changes the result.
DROP TABLE IF EXISTS t_no_share;
CREATE TABLE t_no_share (n Nested(b UInt64, c UInt64)) ENGINE = MergeTree ORDER BY tuple()
    SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
             share_nested_offsets = 0, substream_naming_version = 'namespaced',
             replace_long_file_name_to_hash = 0, ratio_of_defaults_for_sparse_serialization = 1;
INSERT INTO t_no_share VALUES ([1,2],[3,4,5]);

SELECT 'independent offsets';
SELECT n.b, n.c FROM t_no_share;
SELECT n.b.size0, n.c.size0 FROM t_no_share;
-- Data and offsets for each column, and no group level offsets file.
SELECT arraySort(groupArrayDistinct(f)) FROM
    (SELECT arrayJoin(filenames) AS f FROM system.parts_columns
     WHERE database = currentDatabase() AND table = 't_no_share' AND active);
DROP TABLE t_no_share;
