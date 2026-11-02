DROP TABLE IF EXISTS t_null_sub_evolved;

-- A part written before a metadata-only `T` -> `Nullable(T)` ALTER stores non-nullable
-- data and has no `.null` substream. Reading the `.null` subcolumn (as done by
-- optimize_functions_to_subcolumns for isNull/isNotNull/count) must derive it from the
-- present parent column, giving null-map = 0 for the existing rows, not the storage-type
-- default (NULL).

CREATE TABLE t_null_sub_evolved (id UInt8, x UInt256)
ENGINE = MergeTree ORDER BY tuple() SETTINGS auto_statistics_types = '';

INSERT INTO t_null_sub_evolved VALUES (1, 42);

ALTER TABLE t_null_sub_evolved
    MODIFY COLUMN x Nullable(UInt256) DEFAULT NULL
SETTINGS mutations_sync = 2;

SELECT 'compact, DEFAULT NULL';
SELECT count() FROM t_null_sub_evolved WHERE id = 1 AND x IS NULL SETTINGS optimize_functions_to_subcolumns = 1;
SELECT count() FROM t_null_sub_evolved WHERE id = 1 AND x IS NULL SETTINGS optimize_functions_to_subcolumns = 0;
SELECT x, x.null FROM t_null_sub_evolved ORDER BY id;

DROP TABLE t_null_sub_evolved;

-- No DEFAULT, add a genuine NULL in a new part; check isNull/isNotNull/count.
CREATE TABLE t_null_sub_evolved (id UInt8, x UInt256)
ENGINE = MergeTree ORDER BY tuple() SETTINGS auto_statistics_types = '';

INSERT INTO t_null_sub_evolved VALUES (1, 42);
ALTER TABLE t_null_sub_evolved MODIFY COLUMN x Nullable(UInt256) SETTINGS mutations_sync = 2;
INSERT INTO t_null_sub_evolved VALUES (2, NULL);

SELECT 'compact, no default';
SELECT count() FROM t_null_sub_evolved WHERE x IS NULL SETTINGS optimize_functions_to_subcolumns = 1;
SELECT count() FROM t_null_sub_evolved WHERE x IS NULL SETTINGS optimize_functions_to_subcolumns = 0;
SELECT count() FROM t_null_sub_evolved WHERE x IS NOT NULL SETTINGS optimize_functions_to_subcolumns = 1;
SELECT count() FROM t_null_sub_evolved WHERE x IS NOT NULL SETTINGS optimize_functions_to_subcolumns = 0;
SELECT count(x) FROM t_null_sub_evolved SETTINGS optimize_functions_to_subcolumns = 1;
SELECT count(x) FROM t_null_sub_evolved SETTINGS optimize_functions_to_subcolumns = 0;
SELECT id, x, x.null FROM t_null_sub_evolved ORDER BY id;

DROP TABLE t_null_sub_evolved;

-- Wide part.
CREATE TABLE t_null_sub_evolved (id UInt8, x UInt256)
ENGINE = MergeTree ORDER BY tuple() SETTINGS min_bytes_for_wide_part = 0, auto_statistics_types = '';

INSERT INTO t_null_sub_evolved VALUES (1, 42);
ALTER TABLE t_null_sub_evolved MODIFY COLUMN x Nullable(UInt256) SETTINGS mutations_sync = 2;

SELECT 'wide';
SELECT count() FROM t_null_sub_evolved WHERE x IS NULL SETTINGS optimize_functions_to_subcolumns = 1;
SELECT count() FROM t_null_sub_evolved WHERE x IS NULL SETTINGS optimize_functions_to_subcolumns = 0;
SELECT x, x.null FROM t_null_sub_evolved ORDER BY id;

DROP TABLE t_null_sub_evolved;

-- String -> Nullable(String) evolution.
CREATE TABLE t_null_sub_evolved (id UInt8, x String)
ENGINE = MergeTree ORDER BY tuple() SETTINGS auto_statistics_types = '';

INSERT INTO t_null_sub_evolved VALUES (1, 'a');
ALTER TABLE t_null_sub_evolved MODIFY COLUMN x Nullable(String) SETTINGS mutations_sync = 2;
INSERT INTO t_null_sub_evolved VALUES (2, NULL);

SELECT 'string';
SELECT count() FROM t_null_sub_evolved WHERE x IS NULL SETTINGS optimize_functions_to_subcolumns = 1;
SELECT count() FROM t_null_sub_evolved WHERE x IS NULL SETTINGS optimize_functions_to_subcolumns = 0;
SELECT id, x, x.null FROM t_null_sub_evolved ORDER BY id;

DROP TABLE t_null_sub_evolved;

-- Memory engine: MODIFY COLUMN is metadata-only (in-RAM blocks are not rewritten), so a block
-- inserted before the ALTER holds non-nullable data with no `.null` substream. Reading `.null`
-- must derive it from the converted parent (null-map = 0), not crash and not return NULL.
DROP TABLE IF EXISTS t_null_sub_mem;
CREATE TABLE t_null_sub_mem (id UInt8, x UInt256) ENGINE = Memory;

INSERT INTO t_null_sub_mem VALUES (1, 42);
ALTER TABLE t_null_sub_mem MODIFY COLUMN x Nullable(UInt256);
INSERT INTO t_null_sub_mem VALUES (2, NULL);

SELECT 'memory';
SELECT count() FROM t_null_sub_mem WHERE x IS NULL SETTINGS optimize_functions_to_subcolumns = 1;
SELECT count() FROM t_null_sub_mem WHERE x IS NULL SETTINGS optimize_functions_to_subcolumns = 0;
SELECT count() FROM t_null_sub_mem WHERE x IS NOT NULL SETTINGS optimize_functions_to_subcolumns = 1;
SELECT count(x) FROM t_null_sub_mem SETTINGS optimize_functions_to_subcolumns = 1;
SELECT id, x, x.null FROM t_null_sub_mem ORDER BY id;

DROP TABLE t_null_sub_mem;

-- apply_mutations_on_fly: an on-fly UPDATE produces the full `x` in an earlier pipeline step,
-- then a metadata-only MODIFY COLUMN makes it Nullable. The later step reads only `x.null`; it
-- must be derived from the parent produced by the on-fly step (null-map = 0), not default-filled
-- to all-NULL (issue #110555 corner case).
DROP TABLE IF EXISTS t_null_sub_amof;
CREATE TABLE t_null_sub_amof (id UInt8, x UInt8)
ENGINE = MergeTree ORDER BY id SETTINGS auto_statistics_types = '';

INSERT INTO t_null_sub_amof VALUES (1, 5);
SYSTEM STOP MERGES t_null_sub_amof;
ALTER TABLE t_null_sub_amof UPDATE x = 0 WHERE 1 SETTINGS mutations_sync = 0;
ALTER TABLE t_null_sub_amof MODIFY COLUMN x Nullable(UInt8) SETTINGS mutations_sync = 0, alter_sync = 0;
INSERT INTO t_null_sub_amof VALUES (2, NULL);

SELECT 'apply_mutations_on_fly';
SELECT count() FROM t_null_sub_amof WHERE x IS NULL SETTINGS apply_mutations_on_fly = 1, optimize_functions_to_subcolumns = 1;
SELECT count() FROM t_null_sub_amof WHERE x IS NULL SETTINGS apply_mutations_on_fly = 1, optimize_functions_to_subcolumns = 0;
SELECT id, x, x.null FROM t_null_sub_amof ORDER BY id SETTINGS apply_mutations_on_fly = 1;

DROP TABLE t_null_sub_amof;

-- Tuple subcolumn that STILL exists in the old block type after a sibling element's
-- metadata-only type change. Reading `t.a` must extract it directly and must NOT cast the whole
-- tuple (which would throw on the non-convertible old `b` value). Memory engine keeps the
-- pre-ALTER block, so this exercises the old-type-still-has-subcolumn path.
DROP TABLE IF EXISTS t_tuple_elem_mem;
CREATE TABLE t_tuple_elem_mem (id UInt8, t Tuple(a UInt8, b String)) ENGINE = Memory;

INSERT INTO t_tuple_elem_mem VALUES (1, (7, 'x'));
ALTER TABLE t_tuple_elem_mem MODIFY COLUMN t Tuple(a UInt8, b UInt64);

SELECT 'tuple element';
SELECT t.a FROM t_tuple_elem_mem;

DROP TABLE t_tuple_elem_mem;

-- A pending `RENAME COLUMN` leaves the column stored under its previous name in the part while the
-- table metadata already carries the new one. The `.null` subcolumn must still be derived from the
-- parent column that is read alongside it, not filled from the storage-type default.
DROP TABLE IF EXISTS t_null_sub_renamed;
CREATE TABLE t_null_sub_renamed (id UInt8, y UInt8)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 1000000000, auto_statistics_types = '';

INSERT INTO t_null_sub_renamed VALUES (1, 42);
ALTER TABLE t_null_sub_renamed MODIFY COLUMN y Nullable(UInt8) SETTINGS mutations_sync = 2;
SYSTEM STOP MERGES t_null_sub_renamed;
ALTER TABLE t_null_sub_renamed RENAME COLUMN y TO x SETTINGS mutations_sync = 0, alter_sync = 0;

SELECT 'compact, pending rename';
SELECT count() FROM t_null_sub_renamed WHERE x IS NULL SETTINGS optimize_functions_to_subcolumns = 1;
SELECT count() FROM t_null_sub_renamed WHERE x IS NULL SETTINGS optimize_functions_to_subcolumns = 0;
SELECT count(x) FROM t_null_sub_renamed SETTINGS optimize_functions_to_subcolumns = 1;
SELECT id, x, x.null FROM t_null_sub_renamed ORDER BY id;

DROP TABLE t_null_sub_renamed;

CREATE TABLE t_null_sub_renamed (id UInt8, y UInt8)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0, auto_statistics_types = '';

INSERT INTO t_null_sub_renamed VALUES (1, 42);
ALTER TABLE t_null_sub_renamed MODIFY COLUMN y Nullable(UInt8) SETTINGS mutations_sync = 2;
SYSTEM STOP MERGES t_null_sub_renamed;
ALTER TABLE t_null_sub_renamed RENAME COLUMN y TO x SETTINGS mutations_sync = 0, alter_sync = 0;

SELECT 'wide, pending rename';
SELECT count() FROM t_null_sub_renamed WHERE x IS NULL SETTINGS optimize_functions_to_subcolumns = 1;
SELECT count() FROM t_null_sub_renamed WHERE x IS NULL SETTINGS optimize_functions_to_subcolumns = 0;
SELECT count(x) FROM t_null_sub_renamed SETTINGS optimize_functions_to_subcolumns = 1;
SELECT id, x, x.null FROM t_null_sub_renamed ORDER BY id;

DROP TABLE t_null_sub_renamed;
