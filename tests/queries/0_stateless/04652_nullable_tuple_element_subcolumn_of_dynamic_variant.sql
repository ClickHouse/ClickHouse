-- Reading a subcolumn whose own type is Nullable out of a Variant/Dynamic column stored in MergeTree.

SET enable_variant_type = 1;

DROP TABLE IF EXISTS t_dyn;
DROP TABLE IF EXISTS t_dyn_wide;
DROP TABLE IF EXISTS t_var;
DROP TABLE IF EXISTS t_mem;
DROP TABLE IF EXISTS t_json;

-- The requested subcolumn is a Nullable tuple element.
CREATE TABLE t_dyn (id UInt64, value Dynamic) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_dyn VALUES (1, CAST(tuple(CAST(1, 'Nullable(UInt32)'), 's'), 'Tuple(a Nullable(UInt32), b String)'));
INSERT INTO t_dyn VALUES (2, CAST(tuple(CAST(NULL, 'Nullable(UInt32)'), 's'), 'Tuple(a Nullable(UInt32), b String)'));
INSERT INTO t_dyn VALUES (3, 'not a tuple');

SELECT toTypeName(value.`Tuple(a Nullable(UInt32), b String)`.a) FROM t_dyn LIMIT 1;
-- Element value, element NULL, and a row of another variant.
SELECT id, value.`Tuple(a Nullable(UInt32), b String)`.a FROM t_dyn ORDER BY id;
SELECT id, value.`Tuple(a Nullable(UInt32), b String)`.b FROM t_dyn ORDER BY id;
-- The null map subcolumn of the extracted subcolumn.
SELECT id, value.`Tuple(a Nullable(UInt32), b String)`.a.null FROM t_dyn ORDER BY id;
SELECT id FROM t_dyn PREWHERE value.`Tuple(a Nullable(UInt32), b String)`.a = 1;
SELECT sum(value.`Tuple(a Nullable(UInt32), b String)`.a) FROM t_dyn;

-- Same in a wide part.
CREATE TABLE t_dyn_wide (id UInt64, value Dynamic) ENGINE = MergeTree ORDER BY id
    SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
INSERT INTO t_dyn_wide SELECT * FROM t_dyn;
SELECT id, value.`Tuple(a Nullable(UInt32), b String)`.a FROM t_dyn_wide ORDER BY id;

-- Same values from a Memory table, as an oracle for the two queries above.
CREATE TABLE t_mem (id UInt64, value Dynamic) ENGINE = Memory;
INSERT INTO t_mem SELECT * FROM t_dyn;
SELECT id, value.`Tuple(a Nullable(UInt32), b String)`.a FROM t_mem ORDER BY id;

-- Variant shares the same serialization, so it must be fixed too.
CREATE TABLE t_var (id UInt64, value Variant(Tuple(a Nullable(UInt32), b String), String)) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_var VALUES (1, CAST(tuple(CAST(1, 'Nullable(UInt32)'), 's'), 'Tuple(a Nullable(UInt32), b String)'));
INSERT INTO t_var VALUES (2, CAST(tuple(CAST(NULL, 'Nullable(UInt32)'), 's'), 'Tuple(a Nullable(UInt32), b String)'));
INSERT INTO t_var VALUES (3, 'not a tuple');
SELECT toTypeName(value.`Tuple(a Nullable(UInt32), b String)`.a) FROM t_var LIMIT 1;
SELECT id, value.`Tuple(a Nullable(UInt32), b String)`.a FROM t_var ORDER BY id;

-- Other shapes of an intrinsically nullable requested subcolumn.
DROP TABLE IF EXISTS t_shapes;
CREATE TABLE t_shapes (id UInt64, value Dynamic) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_shapes VALUES (1, CAST(tuple(CAST(1, 'Nullable(UInt32)'), CAST(2, 'Nullable(UInt32)')), 'Tuple(a Nullable(UInt32), b Nullable(UInt32))'));
INSERT INTO t_shapes VALUES (2, CAST(tuple(CAST('x', 'LowCardinality(Nullable(String))'), 's'), 'Tuple(a LowCardinality(Nullable(String)), b String)'));
INSERT INTO t_shapes VALUES (3, CAST(tuple(tuple(CAST(1, 'Nullable(UInt32)'))), 'Tuple(a Tuple(x Nullable(UInt32)))'));
INSERT INTO t_shapes VALUES (4, CAST(tuple(CAST(1, 'Nullable(UInt32)'), 's'), 'Tuple(Nullable(UInt32), String)'));
SELECT value.`Tuple(a Nullable(UInt32), b Nullable(UInt32))`.b FROM t_shapes ORDER BY id;
SELECT toTypeName(value.`Tuple(a LowCardinality(Nullable(String)), b String)`.a),
       value.`Tuple(a LowCardinality(Nullable(String)), b String)`.a FROM t_shapes ORDER BY id;
SELECT value.`Tuple(a Tuple(x Nullable(UInt32)))`.a.x FROM t_shapes ORDER BY id;
SELECT value.`Tuple(Nullable(UInt32), String)`.1 FROM t_shapes ORDER BY id;

-- Controls. The nullability added by the extraction must still be removed:
-- here the on-disk element is UInt32 while the subcolumn is exposed as Nullable(UInt32).
DROP TABLE IF EXISTS t_controls;
CREATE TABLE t_controls (id UInt64, value Dynamic) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_controls VALUES (1, CAST(5, 'UInt32'));
INSERT INTO t_controls VALUES (2, CAST(tuple(CAST(1, 'UInt32'), 's'), 'Tuple(a UInt32, b String)'));
INSERT INTO t_controls VALUES (3, CAST(tuple([CAST(1, 'Nullable(UInt32)')]), 'Tuple(a Array(Nullable(UInt32)))'));
INSERT INTO t_controls VALUES (4, CAST(tuple(map('k', CAST(1, 'Nullable(UInt32)'))), 'Tuple(a Map(String, Nullable(UInt32)))'));
SELECT toTypeName(value.UInt32), value.UInt32 FROM t_controls ORDER BY id;
SELECT toTypeName(value.`Tuple(a UInt32, b String)`.a), value.`Tuple(a UInt32, b String)`.a FROM t_controls ORDER BY id;
SELECT value.`Tuple(a Array(Nullable(UInt32)))`.a FROM t_controls ORDER BY id;
SELECT value.`Tuple(a Map(String, Nullable(UInt32)))`.a FROM t_controls ORDER BY id;
-- Both an intrinsically nullable and an extraction-wrapped subcolumn in one query, so that both
-- serializations are built while the same serialization pool is alive.
SELECT value.UInt32, value.`Tuple(a Nullable(UInt32), b String)`.a FROM t_controls ORDER BY id;

-- The shared variant branch always reads into a Nullable column, so its unwrap stays unconditional.
DROP TABLE IF EXISTS t_shared;
CREATE TABLE t_shared (id UInt64, value Dynamic(max_types = 0)) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_shared VALUES (1, CAST(tuple(CAST(1, 'Nullable(UInt32)'), 's'), 'Tuple(a Nullable(UInt32), b String)'));
SELECT toTypeName(value.`Tuple(a Nullable(UInt32), b String)`.a),
       value.`Tuple(a Nullable(UInt32), b String)`.a FROM t_shared;

-- A JSON typed path reaches the same Dynamic subcolumn extraction.
CREATE TABLE t_json (id UInt64, value JSON) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_json VALUES (1, '{"k": 1}');
SELECT value.k.:Int64 FROM t_json;

DROP TABLE t_dyn;
DROP TABLE t_dyn_wide;
DROP TABLE t_var;
DROP TABLE t_mem;
DROP TABLE t_json;
DROP TABLE t_shapes;
DROP TABLE t_controls;
DROP TABLE t_shared;
