-- A part can carry an older type for a column than the table metadata does, because an `ALTER TABLE
-- ... MODIFY COLUMN` is not always applied to the data right away. A mutation that rewrites the whole
-- part re-reads every column at the type in the metadata, so the resulting part must record that type
-- and not the stale one from the source part.

-- Read-in-order on the base table would decline the forced projections in this test
-- (`PROJECTION_NOT_USED`), so disable it: plan shape is not this test's subject.
SET optimize_read_in_order = 0;

SELECT '--- materialize a projection after a type change ---';

DROP TABLE IF EXISTS t_stale_part_type_1;

-- `enable_block_number_column` makes every mutation that goes through the interpreter also materialize
-- `_block_number`, and `auto_statistics_types` keeps the type change itself out of the data rewrite, so
-- the part is still `String` when the projection is materialized.
CREATE TABLE t_stale_part_type_1 (s String) ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0,
         enable_block_number_column = 1, auto_statistics_types = 'tdigest';

INSERT INTO t_stale_part_type_1 SELECT 'str' FROM numbers(1);

ALTER TABLE t_stale_part_type_1 MODIFY COLUMN s Nullable(String) SETTINGS mutations_sync = 1;
ALTER TABLE t_stale_part_type_1 ADD PROJECTION p1 (SELECT s ORDER BY s);
ALTER TABLE t_stale_part_type_1 MATERIALIZE PROJECTION p1 SETTINGS mutations_sync = 1;

SELECT s FROM t_stale_part_type_1;

SELECT column, type FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_stale_part_type_1' AND active AND column = 's';

DROP TABLE t_stale_part_type_1;

SELECT '--- a part detached across the type change ---';

DROP TABLE IF EXISTS t_stale_part_type_2;

CREATE TABLE t_stale_part_type_2 (a String, b String, c String MATERIALIZED concat(a, '!'))
ENGINE = MergeTree ORDER BY a SETTINGS min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0;

INSERT INTO t_stale_part_type_2 VALUES ('x', 'y');

ALTER TABLE t_stale_part_type_2 DETACH PART 'all_1_1_0';
ALTER TABLE t_stale_part_type_2 MODIFY COLUMN b Nullable(String);
ALTER TABLE t_stale_part_type_2 ATTACH PART 'all_1_1_0';

SELECT 'part type before the rewrite';
SELECT column, type FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_stale_part_type_2' AND active AND column = 'b';

ALTER TABLE t_stale_part_type_2 ADD PROJECTION p_ab (SELECT a, b ORDER BY a);

-- `MATERIALIZE COLUMN c` writes `c`, the projection needs `a` and `b` to be read, and together they
-- cover every column of the table, so the mutation rewrites the whole part. `b` is only read, not
-- written by a mutation command, so it is absent from the updated header.
ALTER TABLE t_stale_part_type_2 MATERIALIZE COLUMN c, MATERIALIZE PROJECTION p_ab SETTINGS mutations_sync = 1;

SELECT 'part type after the rewrite';
SELECT column, type FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_stale_part_type_2' AND active AND column = 'b';

SELECT 'data';
SELECT a, b, c FROM t_stale_part_type_2 ORDER BY a;

SELECT 'read from the projection';
SELECT a, b FROM t_stale_part_type_2 ORDER BY a SETTINGS optimize_use_projections = 1, force_optimize_projection = 1;

DROP TABLE t_stale_part_type_2;

SELECT '--- a stale tuple type and its serialization info ---';

DROP TABLE IF EXISTS t_stale_part_type_3;

-- The part also carries a `SerializationInfo` per column, and it describes the type the part has.
-- `SerializationInfoTuple` keeps one entry per tuple element, so the info of the stale `Tuple(x String)`
-- has fewer elements than the `Tuple(x String, y String)` the rewritten part records. Carrying it over
-- makes `DataTypeTuple::getSerialization` read past the end of that list.
CREATE TABLE t_stale_part_type_3 (a String, t Tuple(x String), c String MATERIALIZED concat(a, '!'))
ENGINE = MergeTree ORDER BY a
SETTINGS min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0,
         ratio_of_defaults_for_sparse_serialization = 0.9;

INSERT INTO t_stale_part_type_3 SELECT 'x', tuple('') FROM numbers(100);

ALTER TABLE t_stale_part_type_3 DETACH PART 'all_1_1_0';
ALTER TABLE t_stale_part_type_3 MODIFY COLUMN t Tuple(x String, y String);
ALTER TABLE t_stale_part_type_3 ATTACH PART 'all_1_1_0';

SELECT 'part type before the rewrite';
SELECT column, type FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_stale_part_type_3' AND active AND column = 't';

ALTER TABLE t_stale_part_type_3 ADD PROJECTION p_at (SELECT a, t ORDER BY a);
ALTER TABLE t_stale_part_type_3 MATERIALIZE COLUMN c, MATERIALIZE PROJECTION p_at SETTINGS mutations_sync = 1;

SELECT 'part type after the rewrite';
SELECT column, type FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_stale_part_type_3' AND active AND column = 't';

SELECT 'data';
SELECT DISTINCT a, t, c FROM t_stale_part_type_3;

DROP TABLE t_stale_part_type_3;

SELECT '--- a renamed tuple element and its serialization info ---';

DROP TABLE IF EXISTS t_stale_part_type_4;

-- A tuple element can also be renamed without changing the number of elements, and then the stale
-- `SerializationInfoTuple` has the right shape but the wrong element identities. Everything that merges
-- tuple subinfos - `SerializationInfoTuple::add` and `SerializationInfoTuple::replaceData` - matches the
-- elements by name, so the info of the rewritten part must be keyed by the names of the storage type:
-- otherwise the renamed element looks missing and contributes all-default rows, which makes a later
-- merge pick the sparse serialization for a column that has no default values at all.
CREATE TABLE t_stale_part_type_4 (a String, t Tuple(String), c String MATERIALIZED concat(a, '!'))
ENGINE = MergeTree ORDER BY a
SETTINGS min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0,
         ratio_of_defaults_for_sparse_serialization = 0.9;

INSERT INTO t_stale_part_type_4 SELECT 'x', tuple('v') FROM numbers(100);

ALTER TABLE t_stale_part_type_4 DETACH PART 'all_1_1_0';
ALTER TABLE t_stale_part_type_4 MODIFY COLUMN t Tuple(y String);
ALTER TABLE t_stale_part_type_4 ATTACH PART 'all_1_1_0';

SELECT 'part type before the rewrite';
SELECT column, type FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_stale_part_type_4' AND active AND column = 't';

ALTER TABLE t_stale_part_type_4 ADD PROJECTION p_at (SELECT a, t ORDER BY a);
ALTER TABLE t_stale_part_type_4 MATERIALIZE COLUMN c, MATERIALIZE PROJECTION p_at SETTINGS mutations_sync = 1;

SELECT 'part type after the rewrite';
SELECT column, type FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_stale_part_type_4' AND active AND column = 't';

INSERT INTO t_stale_part_type_4 SELECT 'z', tuple('w') FROM numbers(5);
OPTIMIZE TABLE t_stale_part_type_4 FINAL;

SELECT 'serialization of the merged part';
SELECT column, subcolumns.serializations[indexOf(subcolumns.names, 'y')] FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_stale_part_type_4' AND active AND column = 't';

SELECT 'data';
SELECT count(), countIf(t.y != '') FROM t_stale_part_type_4;

DROP TABLE t_stale_part_type_4;

SELECT '--- a renamed column with a stale type ---';

DROP TABLE IF EXISTS t_stale_part_type_5;

-- The type in the column list of the new part is recorded from three places: the source part column
-- with the same name, the column the mutation renames into this name, and the column a stale part
-- carries under its old name. All three must record the type in storage when the whole part is
-- rewritten. Here `b` is renamed to `d` after the part was detached across the type change, so the
-- part still carries `d` as `String` while the table says `Nullable(String)`, and the rewrite reads it
-- through the rename branch.
CREATE TABLE t_stale_part_type_5 (a String, b String, c String MATERIALIZED concat(a, '!'))
ENGINE = MergeTree ORDER BY a SETTINGS min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0;

INSERT INTO t_stale_part_type_5 VALUES ('x', 'y');

ALTER TABLE t_stale_part_type_5 DETACH PART 'all_1_1_0';
ALTER TABLE t_stale_part_type_5 MODIFY COLUMN b Nullable(String);
ALTER TABLE t_stale_part_type_5 ATTACH PART 'all_1_1_0';
ALTER TABLE t_stale_part_type_5 RENAME COLUMN b TO d SETTINGS mutations_sync = 1;

SELECT 'part type before the rewrite';
SELECT column, type FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_stale_part_type_5' AND active AND column = 'd';

ALTER TABLE t_stale_part_type_5 ADD PROJECTION p_ad (SELECT a, d ORDER BY a);
ALTER TABLE t_stale_part_type_5 MATERIALIZE COLUMN c, MATERIALIZE PROJECTION p_ad SETTINGS mutations_sync = 1;

SELECT 'part type after the rewrite';
SELECT column, type FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_stale_part_type_5' AND active AND column = 'd';

SELECT 'data';
SELECT a, d, c FROM t_stale_part_type_5 ORDER BY a;

SELECT 'read from the projection';
SELECT a, d FROM t_stale_part_type_5 ORDER BY a SETTINGS optimize_use_projections = 1, force_optimize_projection = 1;

DROP TABLE t_stale_part_type_5;
