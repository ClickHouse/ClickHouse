-- Tags: no-fasttest, no-random-merge-tree-settings, no-parallel-replicas
-- no-fasttest: the QBit cases need the QBit type.
-- no-random-merge-tree-settings: every case pins index_granularity so the granule counts are stable.
-- no-parallel-replicas: EXPLAIN output differs for parallel replicas (an extra per-node Granules
-- block).
-- Cases 11-12 and 22-30 of the series started in 04165_skip_index_stale_type_after_alter: parts
-- carrying index files for a column (or a subcolumn parent) they hold no bytes of. One test exceeded
-- the flaky-check runtime limit under sanitizers, so the series is split, keeping the original case
-- numbering.

SET mutations_sync = 0, alter_sync = 0;
-- Statistics part pruning is an independent mechanism that can drop a whole part before any index
-- is read, which would make these assertions measure something other than the skip index.
SET use_statistics_for_part_pruning = 0;

SELECT '-- 11. killed mutation on a column the part carries an index for but no bytes of';
DROP TABLE IF EXISTS t_absent_col;
-- A column TTL expires the column's bytes out of the part while leaving the index files behind, so
-- the part records no type to compare the granule against.
CREATE TABLE t_absent_col (k UInt64, d DateTime, c String TTL d + INTERVAL 1 SECOND,
    INDEX idx c TYPE set(100) GRANULARITY 1)
ENGINE = MergeTree ORDER BY k
SETTINGS index_granularity = 4, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
INSERT INTO t_absent_col SELECT number, '2000-01-01 00:00:00', toString(number * 3) FROM numbers(64);
ALTER TABLE t_absent_col MATERIALIZE TTL SETTINGS mutations_sync = 2, alter_sync = 2;
SELECT count() = 0 FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_absent_col' AND active AND column = 'c';
SELECT sum(secondary_indices_uncompressed_bytes) > 0 FROM system.parts WHERE database = currentDatabase() AND table = 't_absent_col' AND active;
SYSTEM STOP MERGES t_absent_col;
-- The expired column now reads as its type default everywhere, while the granules still hold the
-- pre-expiry values: pruning on them would drop every row this must return.
SELECT count() = 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_absent_col WHERE c = '') WHERE extract(explain, 'Granules: (\d+/\d+)') NOT IN ('', '16/16');
SELECT count() FROM t_absent_col WHERE c = '';
SELECT count() FROM t_absent_col WHERE c = '' SETTINGS use_skip_indexes = 0;
ALTER TABLE t_absent_col MODIFY COLUMN c Nullable(UInt64);
KILL MUTATION WHERE table = 't_absent_col' AND database = currentDatabase() FORMAT Null;
SELECT count() = 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_absent_col WHERE c = 0) WHERE extract(explain, 'Granules: (\d+/\d+)') NOT IN ('', '16/16');
SELECT count() FROM t_absent_col WHERE c = 0;
SELECT count() FROM t_absent_col WHERE c = 0 SETTINGS use_skip_indexes = 0;

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
CREATE TABLE t_absent_sub (k UInt64, d DateTime, p Tuple(x UInt64) TTL d + INTERVAL 1 SECOND,
    INDEX idx p.x TYPE set(100) GRANULARITY 1)
ENGINE = MergeTree ORDER BY k
SETTINGS index_granularity = 4, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
INSERT INTO t_absent_sub SELECT number, '2000-01-01 00:00:00', tuple(number * 3) FROM numbers(64);
ALTER TABLE t_absent_sub MATERIALIZE TTL SETTINGS mutations_sync = 2, alter_sync = 2;
SELECT count() = 0 FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_absent_sub' AND active AND column = 'p';
SELECT sum(secondary_indices_uncompressed_bytes) > 0 FROM system.parts WHERE database = currentDatabase() AND table = 't_absent_sub' AND active;
SYSTEM STOP MERGES t_absent_sub;
-- p.x reads 0 for every row post-expiry, but the granules hold 0, 3, 6, ...: pruning on them keeps
-- only the granule that happens to contain 0 and drops the other 15.
SELECT count() = 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_absent_sub WHERE p.x = 0) WHERE extract(explain, 'Granules: (\d+/\d+)') NOT IN ('', '16/16');
SELECT count() FROM t_absent_sub WHERE p.x = 0;
SELECT count() FROM t_absent_sub WHERE p.x = 0 SETTINGS use_skip_indexes = 0;
ALTER TABLE t_absent_sub MODIFY COLUMN p Tuple(x Nullable(UInt64));
KILL MUTATION WHERE table = 't_absent_sub' AND database = currentDatabase() FORMAT Null;
SELECT count() = 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_absent_sub WHERE p.x = 150) WHERE extract(explain, 'Granules: (\d+/\d+)') NOT IN ('', '16/16');
SELECT count() FROM t_absent_sub WHERE p.x = 150;
SELECT count() FROM t_absent_sub WHERE p.x = 150 SETTINGS use_skip_indexes = 0;

SELECT '-- 23. an absent PHYSICAL column whose name splits onto a custom-serialized neighbour refuses';
-- `b.x` is a real physical column, and its name also splits onto the physical `b`, which carries a
-- custom serialization (Bool) that defines no `x` subcolumn at all. Reading that neighbour as `b.x`'s
-- parent would answer "the part list cannot express this" for a column the part is simply missing,
-- skipping the type check exactly where case 22 requires it. So the escape hatch has to consider the
-- exact name first, and has to require the resolved parent to actually offer the suffix.
DROP TABLE IF EXISTS t_absent_bool_prefix;
CREATE TABLE t_absent_bool_prefix (k UInt64, d DateTime, b Bool, `b.x` UInt8 TTL d + INTERVAL 1 SECOND,
    INDEX idx toString(`b.x`) TYPE set(100) GRANULARITY 1)
ENGINE = MergeTree ORDER BY k
SETTINGS index_granularity = 4, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
INSERT INTO t_absent_bool_prefix SELECT number, '2000-01-01 00:00:00', number % 2, 1 FROM numbers(64);
ALTER TABLE t_absent_bool_prefix MATERIALIZE TTL SETTINGS mutations_sync = 2, alter_sync = 2;
SELECT count() = 0 FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_absent_bool_prefix' AND active AND column = 'b.x';
SELECT sum(secondary_indices_uncompressed_bytes) > 0 FROM system.parts WHERE database = currentDatabase() AND table = 't_absent_bool_prefix' AND active;
SYSTEM STOP MERGES t_absent_bool_prefix;
ALTER TABLE t_absent_bool_prefix MODIFY COLUMN `b.x` Bool;
KILL MUTATION WHERE table = 't_absent_bool_prefix' AND database = currentDatabase() FORMAT Null;
SELECT count() = 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_absent_bool_prefix WHERE toString(`b.x`) = 'false') WHERE extract(explain, 'Granules: (\d+/\d+)') NOT IN ('', '16/16');
SELECT count() FROM t_absent_bool_prefix WHERE toString(`b.x`) = 'false';
SELECT count() FROM t_absent_bool_prefix WHERE toString(`b.x`) = 'false' SETTINGS use_skip_indexes = 0;

-- The suffix requirement carries this shape on its own: `a.b`.x is not a physical column at all, and
-- its SHORTEST split resolves to a custom-serialized `a Bool` offering no `b.x`, while the true parent
-- `a.b` is a longer split with no custom serialization. So the walk must reject the short split on the
-- suffix it does not offer, keep looking, and then refuse - a Tuple element is representable in
-- columns.txt, so the part's silence about `a.b` is a genuinely absent column.
DROP TABLE IF EXISTS t_absent_bool_dotted;
CREATE TABLE t_absent_bool_dotted (k UInt64, d DateTime, a Bool, `a.b` Tuple(x UInt64) TTL d + INTERVAL 1 SECOND,
    INDEX idx `a.b`.x TYPE set(100) GRANULARITY 1)
ENGINE = MergeTree ORDER BY k
SETTINGS index_granularity = 4, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
INSERT INTO t_absent_bool_dotted SELECT number, '2000-01-01 00:00:00', number % 2, tuple(number * 3) FROM numbers(64);
ALTER TABLE t_absent_bool_dotted MATERIALIZE TTL SETTINGS mutations_sync = 2, alter_sync = 2;
SELECT count() = 0 FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_absent_bool_dotted' AND active AND column = 'a.b';
SELECT sum(secondary_indices_uncompressed_bytes) > 0 FROM system.parts WHERE database = currentDatabase() AND table = 't_absent_bool_dotted' AND active;
SYSTEM STOP MERGES t_absent_bool_dotted;
ALTER TABLE t_absent_bool_dotted MODIFY COLUMN `a.b` Tuple(x Nullable(UInt64));
KILL MUTATION WHERE table = 't_absent_bool_dotted' AND database = currentDatabase() FORMAT Null;
SELECT count() = 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_absent_bool_dotted WHERE `a.b`.x = 0) WHERE extract(explain, 'Granules: (\d+/\d+)') NOT IN ('', '16/16');
SELECT count() FROM t_absent_bool_dotted WHERE `a.b`.x = 0;
SELECT count() FROM t_absent_bool_dotted WHERE `a.b`.x = 0 SETTINGS use_skip_indexes = 0;

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
CREATE TABLE t_absent_qbit_sub (k UInt64, d DateTime, vec QBit(Float32, 4) TTL d + INTERVAL 1 SECOND,
    INDEX idx `vec.8` TYPE set(100) GRANULARITY 1)
ENGINE = MergeTree ORDER BY k
SETTINGS index_granularity = 4, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
INSERT INTO t_absent_qbit_sub SELECT number, '2000-01-01 00:00:00',
    arrayMap(x -> toFloat32(number + x), range(4))::QBit(Float32, 4) FROM numbers(64);
ALTER TABLE t_absent_qbit_sub MATERIALIZE TTL SETTINGS mutations_sync = 2, alter_sync = 2;
SELECT count() = 0 FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_absent_qbit_sub' AND active AND column = 'vec';
SELECT sum(secondary_indices_uncompressed_bytes) > 0 FROM system.parts WHERE database = currentDatabase() AND table = 't_absent_qbit_sub' AND active;
SYSTEM STOP MERGES t_absent_qbit_sub;
ALTER TABLE t_absent_qbit_sub MODIFY COLUMN vec QBit(Float64, 4);
KILL MUTATION WHERE table = 't_absent_qbit_sub' AND database = currentDatabase() FORMAT Null;
SELECT count() = 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_absent_qbit_sub WHERE `vec.8` = CAST(unhex('00'), 'FixedString(1)')) WHERE extract(explain, 'Granules: (\d+/\d+)') NOT IN ('', '16/16');
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

SELECT '-- 26. MATERIALIZE INDEX over a metadata-only ADD COLUMN records the column and prunes';
-- The column the index needs is in the table metadata but absent from this wide part, so the part has
-- to record it (at the type the granules were built from) or the index it just wrote can never be
-- used: isPartTypeCompatible has no part-side type to compare.
DROP TABLE IF EXISTS t_materialize_absent;
CREATE TABLE t_materialize_absent (k UInt64, other String) ENGINE = MergeTree ORDER BY k
SETTINGS index_granularity = 4, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
INSERT INTO t_materialize_absent SELECT number, toString(number) FROM numbers(64);
ALTER TABLE t_materialize_absent ADD COLUMN c UInt64 DEFAULT k * 3 SETTINGS mutations_sync = 2, alter_sync = 2;
ALTER TABLE t_materialize_absent ADD INDEX idx c TYPE set(100) GRANULARITY 1 SETTINGS alter_sync = 2;
ALTER TABLE t_materialize_absent MATERIALIZE INDEX idx SETTINGS mutations_sync = 2, alter_sync = 2;
SYSTEM STOP MERGES t_materialize_absent;
SELECT count() > 0 FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_materialize_absent' AND active AND column = 'c';
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_materialize_absent WHERE c = 150) WHERE explain ILIKE '%Granules: 1/16%';
SELECT count() FROM t_materialize_absent WHERE c = 150;
SELECT count() FROM t_materialize_absent WHERE c = 150 SETTINGS use_skip_indexes = 0;

SELECT '-- 26b. the same for a SUBCOLUMN index: the absent PARENT is what gets recorded';
-- The index requires p.x, so what the part must record is the physical parent p. Case 22 is the
-- refusal side of this shape; this is the side where nothing is stale and pruning must work.
DROP TABLE IF EXISTS t_materialize_absent_sub;
CREATE TABLE t_materialize_absent_sub (k UInt64, other String) ENGINE = MergeTree ORDER BY k
SETTINGS index_granularity = 4, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
INSERT INTO t_materialize_absent_sub SELECT number, toString(number) FROM numbers(64);
ALTER TABLE t_materialize_absent_sub ADD COLUMN p Tuple(x UInt64) DEFAULT tuple(k * 3) SETTINGS mutations_sync = 2, alter_sync = 2;
ALTER TABLE t_materialize_absent_sub ADD INDEX idx p.x TYPE set(100) GRANULARITY 1 SETTINGS alter_sync = 2;
ALTER TABLE t_materialize_absent_sub MATERIALIZE INDEX idx SETTINGS mutations_sync = 2, alter_sync = 2;
SYSTEM STOP MERGES t_materialize_absent_sub;
SELECT count() > 0 FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_materialize_absent_sub' AND active AND column = 'p';
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_materialize_absent_sub WHERE p.x = 150) WHERE explain ILIKE '%Granules: 1/16%';
SELECT count() FROM t_materialize_absent_sub WHERE p.x = 150;
SELECT count() FROM t_materialize_absent_sub WHERE p.x = 150 SETTINGS use_skip_indexes = 0;

SELECT '-- 27. MATERIALIZE INDEX on a part that ALREADY has the index files must not record the column';
-- Such a part keeps its granules by hardlink rather than rebuilding them, so recording the column at
-- the current metadata type would hand isPartTypeCompatible a matching type for a granule built under
-- the old one - a stale granule that passes the type check. The column must stay absent and the index
-- must keep refusing, which is what case 11 asserts for the read side.
DROP TABLE IF EXISTS t_rematerialize_absent;
CREATE TABLE t_rematerialize_absent (k UInt64, d DateTime, c String TTL d + INTERVAL 1 SECOND,
    INDEX idx c TYPE set(100) GRANULARITY 1)
ENGINE = MergeTree ORDER BY k
SETTINGS index_granularity = 4, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
INSERT INTO t_rematerialize_absent SELECT number, '2000-01-01 00:00:00', toString(number * 3) FROM numbers(64);
ALTER TABLE t_rematerialize_absent MATERIALIZE TTL SETTINGS mutations_sync = 2, alter_sync = 2;
SELECT count() = 0 FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_rematerialize_absent' AND active AND column = 'c';
SELECT sum(secondary_indices_uncompressed_bytes) > 0 FROM system.parts WHERE database = currentDatabase() AND table = 't_rematerialize_absent' AND active;
SYSTEM STOP MERGES t_rematerialize_absent;
ALTER TABLE t_rematerialize_absent MODIFY COLUMN c Nullable(UInt64);
KILL MUTATION WHERE table = 't_rematerialize_absent' AND database = currentDatabase() FORMAT Null;
SELECT count() FROM t_rematerialize_absent WHERE c = 150;
SELECT count() FROM t_rematerialize_absent WHERE c = 150 SETTINGS use_skip_indexes = 0;
SYSTEM START MERGES t_rematerialize_absent;
ALTER TABLE t_rematerialize_absent MATERIALIZE INDEX idx SETTINGS mutations_sync = 2, alter_sync = 2;
SELECT count() = 0 FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_rematerialize_absent' AND active AND column = 'c';
SELECT count() = 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_rematerialize_absent WHERE c = 150) WHERE extract(explain, 'Granules: (\d+/\d+)') NOT IN ('', '16/16');
SELECT count() FROM t_rematerialize_absent WHERE c = 150;
SELECT count() FROM t_rematerialize_absent WHERE c = 150 SETTINGS use_skip_indexes = 0;

SELECT '-- 28. a second index over the same absent column keeps the column absent';
-- The column list is a property of the part, so every index on the part reads whatever type is
-- recorded there. idx_old keeps its granules by hardlink across the MATERIALIZE INDEX of idx_new, so
-- recording c would hand idx_old a matching type for granules built under the old one. Both indices
-- stay refused: 26 is the shape where recording is safe, this is the shape where it is not.
DROP TABLE IF EXISTS t_two_indices_absent;
CREATE TABLE t_two_indices_absent (k UInt64, d DateTime, c String TTL d + INTERVAL 1 SECOND,
    INDEX idx_old c TYPE set(100) GRANULARITY 1)
ENGINE = MergeTree ORDER BY k
SETTINGS index_granularity = 4, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
INSERT INTO t_two_indices_absent SELECT number, '2000-01-01 00:00:00', toString(number * 3) FROM numbers(64);
ALTER TABLE t_two_indices_absent MATERIALIZE TTL SETTINGS mutations_sync = 2, alter_sync = 2;
SELECT count() = 0 FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_two_indices_absent' AND active AND column = 'c';
SELECT sum(secondary_indices_uncompressed_bytes) > 0 FROM system.parts WHERE database = currentDatabase() AND table = 't_two_indices_absent' AND active;
ALTER TABLE t_two_indices_absent MODIFY COLUMN c Nullable(UInt64);
KILL MUTATION WHERE table = 't_two_indices_absent' AND database = currentDatabase() FORMAT Null;
SYSTEM STOP MERGES t_two_indices_absent;
-- Pre-condition: idx_old refuses, because c is absent and its granules are stale (case 11).
SELECT count() = 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_two_indices_absent WHERE c = 150) WHERE extract(explain, 'Granules: (\d+/\d+)') NOT IN ('', '16/16');
SYSTEM START MERGES t_two_indices_absent;
ALTER TABLE t_two_indices_absent ADD INDEX idx_new c TYPE set(100) GRANULARITY 1 SETTINGS alter_sync = 2;
ALTER TABLE t_two_indices_absent MATERIALIZE INDEX idx_new SETTINGS mutations_sync = 2, alter_sync = 2;
SYSTEM STOP MERGES t_two_indices_absent;
SELECT count() = 0 FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_two_indices_absent' AND active AND column = 'c';
-- Both indices hold files, so a materialization that silently did nothing cannot pass: the
-- assertions below would then be measuring one index instead of two.
SELECT countIf(data_uncompressed_bytes > 0) FROM system.data_skipping_indices WHERE database = currentDatabase() AND table = 't_two_indices_absent';
-- ignore_data_skipping_indices is what isolates the two indices from each other.
SELECT count() = 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_two_indices_absent WHERE c = 150
    SETTINGS ignore_data_skipping_indices = 'idx_new') WHERE extract(explain, 'Granules: (\d+/\d+)') NOT IN ('', '16/16');
SELECT count() = 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_two_indices_absent WHERE c = 150
    SETTINGS ignore_data_skipping_indices = 'idx_old') WHERE extract(explain, 'Granules: (\d+/\d+)') NOT IN ('', '16/16');
SELECT count() FROM t_two_indices_absent WHERE c = 150;
SELECT count() FROM t_two_indices_absent WHERE c = 150 SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_two_indices_absent WHERE c = 150 SETTINGS ignore_data_skipping_indices = 'idx_new';
SELECT count() FROM t_two_indices_absent WHERE c = 150 SETTINGS ignore_data_skipping_indices = 'idx_old';

SELECT '-- 29. a sibling index whose granules this mutation rebuilds does not keep the column absent';
-- Case 28 keeps the column absent because idx_old carries its granules over. Here the same mutation
-- also rebuilds them, so they are written from current data and the column can be recorded - which
-- idx_new needs, or the index it just wrote is unusable. The UPDATE and the MATERIALIZE INDEX are
-- one ALTER, which is the command set the pipeline also sees when two queued mutation entries are
-- squashed. idx_old reads e as well as c, and updating e is what rebuilds it; c's own TTL is removed
-- first so nothing re-expires c and the two effects stay separate. An index rebuilt through a column
-- the mutation writes without naming it is case 30.
DROP TABLE IF EXISTS t_sibling_rebuilt;
CREATE TABLE t_sibling_rebuilt (k UInt64, d DateTime, c String TTL d + INTERVAL 1 SECOND, e UInt64,
    INDEX idx_old (c, e) TYPE set(100) GRANULARITY 1)
ENGINE = MergeTree ORDER BY k
SETTINGS index_granularity = 4, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
INSERT INTO t_sibling_rebuilt SELECT number, '2000-01-01 00:00:00', toString(number * 3), number FROM numbers(64);
ALTER TABLE t_sibling_rebuilt MATERIALIZE TTL SETTINGS mutations_sync = 2, alter_sync = 2;
SELECT count() = 0 FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_sibling_rebuilt' AND active AND column = 'c';
ALTER TABLE t_sibling_rebuilt MODIFY COLUMN c REMOVE TTL SETTINGS alter_sync = 2;
ALTER TABLE t_sibling_rebuilt ADD INDEX idx_new c TYPE set(100) GRANULARITY 1 SETTINGS alter_sync = 2;
ALTER TABLE t_sibling_rebuilt UPDATE e = e + 1 WHERE 1, MATERIALIZE INDEX idx_new
    SETTINGS mutations_sync = 2, alter_sync = 2;
SYSTEM STOP MERGES t_sibling_rebuilt;
-- Both commands share one mutation id, so the pipeline saw them as one command set. Two ids would
-- mean two separate mutations and the case would silently stop covering the shape.
SELECT uniqExact(mutation_id) = 1 FROM system.mutations WHERE database = currentDatabase()
    AND table = 't_sibling_rebuilt' AND command LIKE '%idx_new%';
SELECT count() > 0 FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_sibling_rebuilt' AND active AND column = 'c';
-- Both indices hold files: a materialization that silently did nothing cannot pass.
SELECT countIf(data_uncompressed_bytes > 0) FROM system.data_skipping_indices WHERE database = currentDatabase() AND table = 't_sibling_rebuilt';
-- Both were built from current data, so both must prune. No row holds '150' post-expiry, so a usable
-- index drops every granule: 0/16, and 16/16 is exactly the refusal this case must not see.
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_sibling_rebuilt WHERE c = '150'
    SETTINGS ignore_data_skipping_indices = 'idx_old') WHERE explain ILIKE '%Granules: 0/16%';
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_sibling_rebuilt WHERE c = '150'
    SETTINGS ignore_data_skipping_indices = 'idx_new') WHERE explain ILIKE '%Granules: 0/16%';
SELECT count() FROM t_sibling_rebuilt WHERE c = '150';
SELECT count() FROM t_sibling_rebuilt WHERE c = '150' SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_sibling_rebuilt WHERE c = '';
SELECT count() FROM t_sibling_rebuilt WHERE c = '' SETTINGS use_skip_indexes = 0;

SELECT '-- 30. a sibling index rebuilt through a TTL target it reads does not keep the column absent';
-- Case 29 rebuilds idx_old through a column the UPDATE names. Here the UPDATE names only d, and the
-- index is rebuilt through g, whose own TTL reads d: expiring a column is writing it, so g is
-- rewritten and every index over it with it. g's TTL is 100 years out, so g is rewritten with its
-- values intact rather than expired. c carries no TTL by then, so nothing writes c for its own sake
-- and the recording is what the indices depend on.
DROP TABLE IF EXISTS t_sibling_ttl_rebuilt;
CREATE TABLE t_sibling_ttl_rebuilt (k UInt64, d DateTime, c String TTL d + INTERVAL 1 SECOND,
    g String TTL d + INTERVAL 100 YEAR, INDEX idx_old (c, g) TYPE set(100) GRANULARITY 1)
ENGINE = MergeTree ORDER BY k
SETTINGS index_granularity = 4, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
INSERT INTO t_sibling_ttl_rebuilt SELECT number, '2000-01-01 00:00:00', toString(number * 3), toString(number) FROM numbers(64);
ALTER TABLE t_sibling_ttl_rebuilt MATERIALIZE TTL SETTINGS mutations_sync = 2, alter_sync = 2;
SELECT count() = 0 FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_sibling_ttl_rebuilt' AND active AND column = 'c';
SELECT count() > 0 FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_sibling_ttl_rebuilt' AND active AND column = 'g';
ALTER TABLE t_sibling_ttl_rebuilt MODIFY COLUMN c REMOVE TTL SETTINGS alter_sync = 2;
ALTER TABLE t_sibling_ttl_rebuilt ADD INDEX idx_new c TYPE set(100) GRANULARITY 1 SETTINGS alter_sync = 2;
ALTER TABLE t_sibling_ttl_rebuilt UPDATE d = toDateTime('2100-01-01 00:00:00') WHERE 1, MATERIALIZE INDEX idx_new
    SETTINGS mutations_sync = 2, alter_sync = 2;
SYSTEM STOP MERGES t_sibling_ttl_rebuilt;
-- Both commands share one mutation id, so the pipeline saw them as one command set. Two ids would
-- mean two separate mutations and the case would silently stop covering the shape.
SELECT uniqExact(mutation_id) = 1 FROM system.mutations WHERE database = currentDatabase()
    AND table = 't_sibling_ttl_rebuilt' AND command LIKE '%idx_new%';
SELECT count() > 0 FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_sibling_ttl_rebuilt' AND active AND column = 'c';
-- g keeps its values, so idx_old was rebuilt from data rather than emptied.
SELECT count() = 64 FROM t_sibling_ttl_rebuilt WHERE g != '';
-- Both indices hold files: a materialization that silently did nothing cannot pass.
SELECT countIf(data_uncompressed_bytes > 0) FROM system.data_skipping_indices WHERE database = currentDatabase() AND table = 't_sibling_ttl_rebuilt';
-- Both were built from current data, so both must prune, and no row holds '150' after the expiry:
-- 0/16 for each, where 16/16 would be the refusal this case must not see.
SELECT count() = 1 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_sibling_ttl_rebuilt WHERE c = '150'
    SETTINGS ignore_data_skipping_indices = 'idx_new') WHERE extract(explain, 'Granules: (\d+/\d+)') = '0/16';
SELECT count() = 1 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_sibling_ttl_rebuilt WHERE c = '150'
    SETTINGS ignore_data_skipping_indices = 'idx_old') WHERE extract(explain, 'Granules: (\d+/\d+)') = '0/16';
SELECT count() FROM t_sibling_ttl_rebuilt WHERE c = '150';
SELECT count() FROM t_sibling_ttl_rebuilt WHERE c = '150' SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_sibling_ttl_rebuilt WHERE c = '';
SELECT count() FROM t_sibling_ttl_rebuilt WHERE c = '' SETTINGS use_skip_indexes = 0;

DROP TABLE t_absent_col;
DROP TABLE t_pre_add_index;
DROP TABLE t_materialized_index;
DROP TABLE t_absent_sub;
DROP TABLE t_absent_bool_prefix;
DROP TABLE t_absent_bool_dotted;
DROP TABLE t_absent_qbit_sub;
DROP TABLE t_keep_qbit_sub;
DROP TABLE t_materialize_absent;
DROP TABLE t_materialize_absent_sub;
DROP TABLE t_rematerialize_absent;
DROP TABLE t_two_indices_absent;
DROP TABLE t_sibling_rebuilt;
DROP TABLE t_sibling_ttl_rebuilt;
