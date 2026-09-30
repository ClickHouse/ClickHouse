-- Tags: no-fasttest, no-random-merge-tree-settings, no-parallel-replicas
-- no-fasttest: the QBit cases need the QBit type.
-- no-random-merge-tree-settings: every case pins index_granularity so the granule counts are stable.
-- no-parallel-replicas: EXPLAIN output differs for parallel replicas (an extra per-node Granules
-- block).
-- Cases 11-12 and 22-25 of the series started in 04165_skip_index_stale_type_after_alter: parts
-- carrying index files for a column (or a subcolumn parent) they hold no bytes of. One test exceeded
-- the flaky-check runtime limit under sanitizers, so the series is split, keeping the original case
-- numbering.

SET mutations_sync = 0, alter_sync = 0;
-- Statistics part pruning is an independent mechanism that can drop a whole part before any index
-- is read, which would make these assertions measure something other than the skip index.
SET use_statistics_for_part_pruning = 0;

SELECT '-- 11. killed mutation on a column the part carries an index for but no bytes of';
DROP TABLE IF EXISTS t_absent_col;
CREATE TABLE t_absent_col (k UInt64, other String) ENGINE = MergeTree ORDER BY k
SETTINGS index_granularity = 4, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
INSERT INTO t_absent_col SELECT number, toString(number) FROM numbers(64);
-- A DEFAULT column added to a wide part is metadata-only, so MATERIALIZE INDEX writes the index
-- files without adding the column to the part: the part records no type to compare against.
ALTER TABLE t_absent_col ADD COLUMN c String DEFAULT toString(k * 3) SETTINGS mutations_sync = 2, alter_sync = 2;
ALTER TABLE t_absent_col ADD INDEX idx c TYPE set(100) GRANULARITY 1 SETTINGS alter_sync = 2;
ALTER TABLE t_absent_col MATERIALIZE INDEX idx SETTINGS mutations_sync = 2, alter_sync = 2;
SELECT count() = 0 FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_absent_col' AND active AND column = 'c';
SYSTEM STOP MERGES t_absent_col;
ALTER TABLE t_absent_col MODIFY COLUMN c Nullable(UInt64);
KILL MUTATION WHERE table = 't_absent_col' AND database = currentDatabase() FORMAT Null;
SELECT count() FROM t_absent_col WHERE c = 150;

SELECT '-- 12. control: an absent column costs no pruning when the part has no index files';
DROP TABLE IF EXISTS t_pre_add_index;
CREATE TABLE t_pre_add_index (k UInt64, c UInt64) ENGINE = MergeTree ORDER BY k
SETTINGS index_granularity = 4, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
INSERT INTO t_pre_add_index SELECT number, number * 3 FROM numbers(64);
SYSTEM STOP MERGES t_pre_add_index;
ALTER TABLE t_pre_add_index ADD INDEX idx c TYPE set(100) GRANULARITY 1 SETTINGS alter_sync = 2;
SELECT count() FROM t_pre_add_index WHERE c = 150;
DROP TABLE IF EXISTS t_materialized_index;
CREATE TABLE t_materialized_index (k UInt64, c UInt64) ENGINE = MergeTree ORDER BY k
SETTINGS index_granularity = 4, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
INSERT INTO t_materialized_index SELECT number, number * 3 FROM numbers(64);
ALTER TABLE t_materialized_index ADD INDEX idx c TYPE set(100) GRANULARITY 1 SETTINGS alter_sync = 2;
ALTER TABLE t_materialized_index MATERIALIZE INDEX idx SETTINGS mutations_sync = 2, alter_sync = 2;
SYSTEM STOP MERGES t_materialized_index;
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_materialized_index WHERE c = 150) WHERE explain ILIKE '%Granules: 1/16%';
SELECT count() FROM t_materialized_index WHERE c = 150;

SELECT '-- 22. a SUBCOLUMN whose parent the part does not carry at all must still refuse';
-- Case 11 with a subcolumn requirement: the part holds index files for p.x but no p column, so the
-- part records no type to compare against and the granule holds bytes of the old type. This is the
-- shape that separates "the part cannot express this subcolumn" from "the parent is simply absent" -
-- a guard keyed on parent existence alone would skip the type check here and prune wrongly.
DROP TABLE IF EXISTS t_absent_sub;
CREATE TABLE t_absent_sub (k UInt64, other String) ENGINE = MergeTree ORDER BY k
SETTINGS index_granularity = 4, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
INSERT INTO t_absent_sub SELECT number, toString(number) FROM numbers(64);
ALTER TABLE t_absent_sub ADD COLUMN p Tuple(x UInt64) DEFAULT tuple(k * 3) SETTINGS mutations_sync = 2, alter_sync = 2;
ALTER TABLE t_absent_sub ADD INDEX idx p.x TYPE set(100) GRANULARITY 1 SETTINGS alter_sync = 2;
ALTER TABLE t_absent_sub MATERIALIZE INDEX idx SETTINGS mutations_sync = 2, alter_sync = 2;
SELECT count() = 0 FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_absent_sub' AND active AND column = 'p';
SYSTEM STOP MERGES t_absent_sub;
ALTER TABLE t_absent_sub MODIFY COLUMN p Tuple(x Nullable(UInt64));
KILL MUTATION WHERE table = 't_absent_sub' AND database = currentDatabase() FORMAT Null;
SELECT count() FROM t_absent_sub WHERE p.x = 150;
SELECT count() FROM t_absent_sub WHERE p.x = 150 SETTINGS use_skip_indexes = 0;

SELECT '-- 23. an absent PHYSICAL column whose name splits onto a custom-serialized neighbour refuses';
-- `b.x` is a real physical column, and its name also splits onto the physical `b`, which carries a
-- custom serialization (Bool) that defines no `x` subcolumn at all. Reading that neighbour as `b.x`'s
-- parent would answer "the part list cannot express this" for a column the part is simply missing,
-- skipping the type check exactly where case 22 requires it. So the escape hatch has to consider the
-- exact name first, and has to require the resolved parent to actually offer the suffix.
DROP TABLE IF EXISTS t_absent_bool_prefix;
CREATE TABLE t_absent_bool_prefix (k UInt64, b Bool) ENGINE = MergeTree ORDER BY k
SETTINGS index_granularity = 4, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
INSERT INTO t_absent_bool_prefix SELECT number, number % 2 FROM numbers(64);
ALTER TABLE t_absent_bool_prefix ADD COLUMN `b.x` UInt8 DEFAULT 1 SETTINGS mutations_sync = 2, alter_sync = 2;
ALTER TABLE t_absent_bool_prefix ADD INDEX idx toString(`b.x`) TYPE set(100) GRANULARITY 1 SETTINGS alter_sync = 2;
ALTER TABLE t_absent_bool_prefix MATERIALIZE INDEX idx SETTINGS mutations_sync = 2, alter_sync = 2;
SELECT count() = 0 FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_absent_bool_prefix' AND active AND column = 'b.x';
SYSTEM STOP MERGES t_absent_bool_prefix;
ALTER TABLE t_absent_bool_prefix MODIFY COLUMN `b.x` Bool;
KILL MUTATION WHERE table = 't_absent_bool_prefix' AND database = currentDatabase() FORMAT Null;
SELECT count() FROM t_absent_bool_prefix WHERE toString(`b.x`) = 'true';
SELECT count() FROM t_absent_bool_prefix WHERE toString(`b.x`) = 'true' SETTINGS use_skip_indexes = 0;

-- The suffix requirement carries this shape on its own: `a.b`.x is not a physical column at all, and
-- its SHORTEST split resolves to a custom-serialized `a Bool` offering no `b.x`, while the true parent
-- `a.b` is a longer split with no custom serialization. So the walk must reject the short split on the
-- suffix it does not offer, keep looking, and then refuse - a Tuple element is representable in
-- columns.txt, so the part's silence about `a.b` is a genuinely absent column.
DROP TABLE IF EXISTS t_absent_bool_dotted;
CREATE TABLE t_absent_bool_dotted (k UInt64, a Bool) ENGINE = MergeTree ORDER BY k
SETTINGS index_granularity = 4, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
INSERT INTO t_absent_bool_dotted SELECT number, number % 2 FROM numbers(64);
ALTER TABLE t_absent_bool_dotted ADD COLUMN `a.b` Tuple(x UInt64) DEFAULT tuple(k * 3) SETTINGS mutations_sync = 2, alter_sync = 2;
ALTER TABLE t_absent_bool_dotted ADD INDEX idx `a.b`.x TYPE set(100) GRANULARITY 1 SETTINGS alter_sync = 2;
ALTER TABLE t_absent_bool_dotted MATERIALIZE INDEX idx SETTINGS mutations_sync = 2, alter_sync = 2;
SELECT count() = 0 FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_absent_bool_dotted' AND active AND column = 'a.b';
SYSTEM STOP MERGES t_absent_bool_dotted;
ALTER TABLE t_absent_bool_dotted MODIFY COLUMN `a.b` Tuple(x Nullable(UInt64));
KILL MUTATION WHERE table = 't_absent_bool_dotted' AND database = currentDatabase() FORMAT Null;
SELECT count() FROM t_absent_bool_dotted WHERE `a.b`.x = 150;
SELECT count() FROM t_absent_bool_dotted WHERE `a.b`.x = 150 SETTINGS use_skip_indexes = 0;

SELECT '-- 24. a SERIALIZATION-DEFINED subcolumn whose parent the part does not carry refuses too';
-- Case 22 for the serialization-defined subcolumns that case 21 (in
-- 04871_skip_index_stale_type_subcolumns) lets through: `vec.8` is a `QBit` bit plane, defined by the
-- parent's custom serialization - which columns.txt DOES round-trip here, because QBit sets it from
-- its own type (unlike Quantized, whose serialization comes from the codec that columns.txt drops).
-- So the reason to refuse is not unrepresentability but parent ABSENCE: this part holds no `vec` at
-- all, and its granule was written for `QBit(Float32, 4)`. Waving the subcolumn through on the
-- strength of its parent's serialization would skip the type check and prune with a stale granule.
-- The backticked spelling is what makes the index require the SUBCOLUMN `vec.8`: it is one
-- identifier, so it resolves against the subcolumn-aware column list, while an unbackticked `vec.8`
-- is the dot operator and requires the physical parent `vec` instead.
DROP TABLE IF EXISTS t_absent_qbit_sub;
CREATE TABLE t_absent_qbit_sub (k UInt64, other String) ENGINE = MergeTree ORDER BY k
SETTINGS index_granularity = 4, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
INSERT INTO t_absent_qbit_sub SELECT number, toString(number) FROM numbers(64);
ALTER TABLE t_absent_qbit_sub ADD COLUMN vec QBit(Float32, 4)
DEFAULT arrayMap(x -> toFloat32(k + x), range(4))::QBit(Float32, 4) SETTINGS mutations_sync = 2, alter_sync = 2;
ALTER TABLE t_absent_qbit_sub ADD INDEX idx `vec.8` TYPE set(100) GRANULARITY 1 SETTINGS alter_sync = 2;
ALTER TABLE t_absent_qbit_sub MATERIALIZE INDEX idx SETTINGS mutations_sync = 2, alter_sync = 2;
SELECT count() = 0 FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_absent_qbit_sub' AND active AND column = 'vec';
SYSTEM STOP MERGES t_absent_qbit_sub;
ALTER TABLE t_absent_qbit_sub MODIFY COLUMN vec QBit(Float64, 4);
KILL MUTATION WHERE table = 't_absent_qbit_sub' AND database = currentDatabase() FORMAT Null;
SELECT count() FROM t_absent_qbit_sub WHERE `vec.8` = CAST(unhex('00'), 'FixedString(1)');
SELECT count() FROM t_absent_qbit_sub WHERE `vec.8` = CAST(unhex('00'), 'FixedString(1)') SETTINGS use_skip_indexes = 0;

SELECT '-- 25. over-fire control: a backticked QBit subcolumn index prunes when the part carries the parent';
-- The other side of case 24: same backticked `vec.8` index, but the parent is present and no type is
-- stale, so the index must still prune. Case 24 alone cannot tell a correct refusal apart from this
-- spelling never pruning at all - only the pair does. `04403` covers the UNBACKTICKED `vec.8`, which
-- is the dot operator and so requires the physical parent instead of this subcolumn.
DROP TABLE IF EXISTS t_keep_qbit_sub;
CREATE TABLE t_keep_qbit_sub (k UInt64, vec QBit(Float32, 4),
    INDEX idx `vec.8` TYPE set(100) GRANULARITY 1)
ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 4;
INSERT INTO t_keep_qbit_sub SELECT number, arrayMap(x -> toFloat32(number + x), range(4))::QBit(Float32, 4) FROM numbers(64);
SYSTEM STOP MERGES t_keep_qbit_sub;
SELECT count() > 0 FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_keep_qbit_sub' AND active AND column = 'vec';
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_keep_qbit_sub WHERE `vec.8` = CAST(unhex('02'), 'FixedString(1)')) WHERE explain ILIKE '%Granules: 1/16%';
SELECT count() FROM t_keep_qbit_sub WHERE `vec.8` = CAST(unhex('02'), 'FixedString(1)');
SELECT count() FROM t_keep_qbit_sub WHERE `vec.8` = CAST(unhex('02'), 'FixedString(1)') SETTINGS use_skip_indexes = 0;

DROP TABLE t_absent_col;
DROP TABLE t_pre_add_index;
DROP TABLE t_materialized_index;
DROP TABLE t_absent_sub;
DROP TABLE t_absent_bool_prefix;
DROP TABLE t_absent_bool_dotted;
DROP TABLE t_absent_qbit_sub;
DROP TABLE t_keep_qbit_sub;
