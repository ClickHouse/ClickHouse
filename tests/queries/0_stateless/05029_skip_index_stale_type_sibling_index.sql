-- Tags: no-random-merge-tree-settings, no-parallel-replicas
-- no-random-merge-tree-settings: every case pins index_granularity so the granule counts are stable.
-- no-parallel-replicas: EXPLAIN output differs for parallel replicas (an extra per-node Granules
-- block).
-- Cases 28-29 and 36 of the series started in 04165_skip_index_stale_type_after_alter and continued in
-- 04869_skip_index_stale_type_absent_column: a part carries index files for a column it holds no
-- bytes of, and a SECOND index on the part decides whether the mutation may record that column.
-- The series is split across files because one test exceeded the flaky-check runtime limit under
-- sanitizers; the original case numbering is kept.

SET mutations_sync = 0, alter_sync = 0;
-- Statistics part pruning is an independent mechanism that can drop a whole part before any index
-- is read, which would make these assertions measure something other than the skip index.
SET use_statistics_for_part_pruning = 0;

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

SELECT '-- 28b. the same when the sibling reads a SUBCOLUMN whose name prefix is another column';
-- idx_old requires `a.b.x`, a subcolumn of `a.b`, while the shorter prefix `a` is a column of its own.
-- `a.b` must stay absent: its default changed after it expired, so idx_old's granules are stale.
DROP TABLE IF EXISTS t_two_indices_dotted;
CREATE TABLE t_two_indices_dotted (k UInt64, d DateTime, a Bool, `a.b` Tuple(x UInt64) TTL d + INTERVAL 1 SECOND,
    INDEX idx_old `a.b.x` TYPE set(100) GRANULARITY 1)
ENGINE = MergeTree ORDER BY k
SETTINGS index_granularity = 4, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
INSERT INTO t_two_indices_dotted SELECT number, '2000-01-01 00:00:00', 1, tuple(number * 3) FROM numbers(64);
ALTER TABLE t_two_indices_dotted MATERIALIZE TTL SETTINGS mutations_sync = 2, alter_sync = 2;
SELECT count() = 0 FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_two_indices_dotted' AND active AND column = 'a.b';
ALTER TABLE t_two_indices_dotted MODIFY COLUMN `a.b` REMOVE TTL SETTINGS alter_sync = 2;
ALTER TABLE t_two_indices_dotted MODIFY COLUMN `a.b` Tuple(x UInt64) DEFAULT tuple(toUInt64(999)) SETTINGS alter_sync = 2;
ALTER TABLE t_two_indices_dotted ADD INDEX idx_new tupleElement(`a.b`, 1) TYPE set(100) GRANULARITY 1 SETTINGS alter_sync = 2;
ALTER TABLE t_two_indices_dotted MATERIALIZE INDEX idx_new SETTINGS mutations_sync = 2, alter_sync = 2;
SYSTEM STOP MERGES t_two_indices_dotted;
SELECT count() = 0 FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_two_indices_dotted' AND active AND column = 'a.b';
SELECT countIf(data_uncompressed_bytes > 0) FROM system.data_skipping_indices WHERE database = currentDatabase() AND table = 't_two_indices_dotted';
SELECT count() FROM t_two_indices_dotted WHERE `a.b.x` = 999 SETTINGS ignore_data_skipping_indices = 'idx_new';
SELECT count() FROM t_two_indices_dotted WHERE `a.b.x` = 999 SETTINGS use_skip_indexes = 0;

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
-- `system.mutations` has a row per command, so both the `UPDATE` row and the `MATERIALIZE INDEX` row
-- are selected, and they must share one mutation id: the pipeline saw them as one command set. Two ids would
-- mean two separate mutations and the case would silently stop covering the shape.
SELECT count() = 2 AND uniqExact(mutation_id) = 1 FROM system.mutations WHERE database = currentDatabase()
    AND table = 't_sibling_rebuilt' AND (command LIKE '%UPDATE %' OR command LIKE '%idx_new%');
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

SELECT '-- 36. a mutation that rewrites the whole part rebuilds a sibling index refused for its column type';
-- idx_new reads every column, so materializing it over the absent c rewrites the whole part, and the
-- rewrite records dt at its new timezone. idx_old was built under UTC and is refused until then; it
-- must be rebuilt, not carried over: its old granules hold hour 0 for every row.
DROP TABLE IF EXISTS t_full_rewrite_stale;
CREATE TABLE t_full_rewrite_stale (k UInt64, dt DateTime('UTC'), INDEX idx_old toHour(dt) TYPE set(100) GRANULARITY 1)
ENGINE = MergeTree ORDER BY k
SETTINGS index_granularity = 4, index_granularity_bytes = 0, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
    packed_skip_index_max_bytes = 0;
INSERT INTO t_full_rewrite_stale SELECT number, toDateTime('2020-01-01 00:00:00', 'UTC') FROM numbers(64);
ALTER TABLE t_full_rewrite_stale MODIFY COLUMN dt DateTime('Asia/Tokyo') SETTINGS mutations_sync = 2, alter_sync = 2;
ALTER TABLE t_full_rewrite_stale ADD COLUMN c UInt64 DEFAULT k * 3 SETTINGS alter_sync = 2;
ALTER TABLE t_full_rewrite_stale ADD INDEX idx_new (c, k, dt) TYPE set(100) GRANULARITY 1 SETTINGS alter_sync = 2;
SELECT type FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_full_rewrite_stale' AND active AND column = 'dt';
ALTER TABLE t_full_rewrite_stale MATERIALIZE INDEX idx_new SETTINGS mutations_sync = 2, alter_sync = 2;
SELECT type FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_full_rewrite_stale' AND active AND column = 'dt';
SELECT countIf(data_uncompressed_bytes > 0) FROM system.data_skipping_indices WHERE database = currentDatabase() AND table = 't_full_rewrite_stale';
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_full_rewrite_stale WHERE toHour(dt) = 0
    SETTINGS ignore_data_skipping_indices = 'idx_new') WHERE explain ILIKE '%Granules: 0/16%';
SELECT count() FROM t_full_rewrite_stale WHERE toHour(dt) = 9 SETTINGS ignore_data_skipping_indices = 'idx_new';
SELECT count() FROM t_full_rewrite_stale WHERE toHour(dt) = 9 SETTINGS use_skip_indexes = 0;

SELECT '-- 36b. the same when an UPDATE of another column is what rewrites the whole part';
DROP TABLE IF EXISTS t_full_rewrite_stale_update;
CREATE TABLE t_full_rewrite_stale_update (k UInt64, dt DateTime('UTC'), v UInt64,
    INDEX idx_old toHour(dt) TYPE set(100) GRANULARITY 1, INDEX idx_all (k, v, dt) TYPE set(100) GRANULARITY 1)
ENGINE = MergeTree ORDER BY k
SETTINGS index_granularity = 4, index_granularity_bytes = 0, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
    packed_skip_index_max_bytes = 0;
INSERT INTO t_full_rewrite_stale_update SELECT number, toDateTime('2020-01-01 00:00:00', 'UTC'), number FROM numbers(64);
ALTER TABLE t_full_rewrite_stale_update MODIFY COLUMN dt DateTime('Asia/Tokyo') SETTINGS mutations_sync = 2, alter_sync = 2;
ALTER TABLE t_full_rewrite_stale_update UPDATE v = v + 1 WHERE 1 SETTINGS mutations_sync = 2, alter_sync = 2;
SELECT type FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_full_rewrite_stale_update' AND active AND column = 'dt';
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_full_rewrite_stale_update WHERE toHour(dt) = 0
    SETTINGS ignore_data_skipping_indices = 'idx_all') WHERE explain ILIKE '%Granules: 0/16%';
SELECT count() FROM t_full_rewrite_stale_update WHERE toHour(dt) = 9 SETTINGS ignore_data_skipping_indices = 'idx_all';
SELECT count() FROM t_full_rewrite_stale_update WHERE toHour(dt) = 9 SETTINGS use_skip_indexes = 0;

DROP TABLE t_two_indices_absent;
DROP TABLE t_two_indices_dotted;
DROP TABLE t_sibling_rebuilt;
DROP TABLE t_full_rewrite_stale;
DROP TABLE t_full_rewrite_stale_update;
