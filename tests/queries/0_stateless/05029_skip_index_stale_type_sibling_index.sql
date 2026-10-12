-- Tags: no-random-merge-tree-settings, no-parallel-replicas
-- no-random-merge-tree-settings: every case pins index_granularity so the granule counts are stable.
-- no-parallel-replicas: EXPLAIN output differs for parallel replicas (an extra per-node Granules
-- block).
-- Cases 28-29 of the series started in 04165_skip_index_stale_type_after_alter and continued in
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

DROP TABLE t_two_indices_absent;
DROP TABLE t_sibling_rebuilt;
