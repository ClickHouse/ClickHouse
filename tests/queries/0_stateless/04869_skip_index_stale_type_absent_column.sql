-- Tags: no-random-merge-tree-settings, no-parallel-replicas
-- no-random-merge-tree-settings: every case pins index_granularity so the granule counts are stable.
-- no-parallel-replicas: EXPLAIN output differs for parallel replicas (an extra per-node Granules
-- block).
-- Cases 11-12 and 26-27 of the series started in 04165_skip_index_stale_type_after_alter: parts
-- carrying index files for a column they hold no bytes of. One test exceeded the flaky-check
-- runtime limit under sanitizers, so the series is split across files, keeping the original case
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

DROP TABLE t_absent_col;
DROP TABLE t_pre_add_index;
DROP TABLE t_materialized_index;
DROP TABLE t_materialize_absent;
DROP TABLE t_materialize_absent_sub;
DROP TABLE t_rematerialize_absent;
