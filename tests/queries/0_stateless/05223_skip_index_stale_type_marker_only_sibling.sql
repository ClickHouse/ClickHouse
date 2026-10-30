-- Tags: no-random-merge-tree-settings, no-parallel-replicas
-- no-random-merge-tree-settings: every case pins index_granularity so the granule counts are stable,
-- and relies on skip_empty_columns_on_insert to leave a column marker-only.
-- no-parallel-replicas: EXPLAIN output differs for parallel replicas (an extra per-node Granules
-- block).
-- Marker-only variants of cases 33 and 34 from 05030_skip_index_stale_type_index_lifecycle: the
-- sibling column the mutation changes is not stored in the part at all, only recorded as a
-- missing-column marker in its serialization infos (`skip_empty_columns_on_insert`).
-- `MutationsInterpreter::prepare` reads the column's old type from that marker when deciding
-- whether `MODIFY COLUMN` rebuilds the indices over it, and rebuilds them on `CLEAR COLUMN` of a
-- marker-only column just as it does for a stored one; the rebuilt-index predictor in
-- `splitAndModifyMutationCommands` must take the same decision, or it keeps the absent carrier
-- `c` out of the part and leaves the freshly materialized `idx_new` inert.

SET mutations_sync = 0, alter_sync = 0;
-- Statistics part pruning is an independent mechanism that can drop a whole part before any index
-- is read, which would make these assertions measure something other than the skip index.
SET use_statistics_for_part_pruning = 0;

SELECT '-- 33b. a sibling index rebuilt by MODIFY COLUMN of a marker-only column does not keep the column absent';
DROP TABLE IF EXISTS t_sibling_marker_modify;
CREATE TABLE t_sibling_marker_modify (k UInt64, d DateTime, c String TTL d + INTERVAL 1 SECOND,
    g UInt64, INDEX idx_old (c, g) TYPE set(100) GRANULARITY 1)
ENGINE = MergeTree ORDER BY k
SETTINGS index_granularity = 4, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
    ratio_of_defaults_for_sparse_serialization = 1.0,
    skip_empty_columns_on_insert = 1, serialization_info_version = 'with_missing_columns';
-- `g` is all type-default, so the part records it as a marker only.
INSERT INTO t_sibling_marker_modify SELECT number, '2000-01-01 00:00:00', toString(number * 3), 0 FROM numbers(64);
SELECT count() = 0 FROM system.parts_columns WHERE database = currentDatabase()
    AND table = 't_sibling_marker_modify' AND active AND column = 'g';
ALTER TABLE t_sibling_marker_modify MATERIALIZE TTL SETTINGS mutations_sync = 2, alter_sync = 2;
SELECT count() = 0 FROM system.parts_columns WHERE database = currentDatabase()
    AND table = 't_sibling_marker_modify' AND active AND column = 'c';
-- The marker survives the mutation, so `g` is still marker-only when the type changes below.
SELECT count() = 0 FROM system.parts_columns WHERE database = currentDatabase()
    AND table = 't_sibling_marker_modify' AND active AND column = 'g';
ALTER TABLE t_sibling_marker_modify MODIFY COLUMN c REMOVE TTL SETTINGS alter_sync = 2;
ALTER TABLE t_sibling_marker_modify ADD INDEX idx_new c TYPE set(100) GRANULARITY 1 SETTINGS alter_sync = 2;
-- Same batching as case 33: `MODIFY COLUMN` gets a mutation entry of its own, and stopping the
-- executor while both are submitted makes the selector squash them into one command set.
SYSTEM STOP MERGES t_sibling_marker_modify;
ALTER TABLE t_sibling_marker_modify MODIFY COLUMN g String SETTINGS mutations_sync = 0, alter_sync = 0;
ALTER TABLE t_sibling_marker_modify MATERIALIZE INDEX idx_new SETTINGS mutations_sync = 0, alter_sync = 0;
SYSTEM START MERGES t_sibling_marker_modify;
ALTER TABLE t_sibling_marker_modify UPDATE d = d WHERE 0 SETTINGS mutations_sync = 2, alter_sync = 2;
SYSTEM STOP MERGES t_sibling_marker_modify;
SELECT count() > 0 FROM system.parts_columns WHERE database = currentDatabase()
    AND table = 't_sibling_marker_modify' AND active AND column = 'c';
SELECT count() = 64 FROM t_sibling_marker_modify WHERE g = '0';
SELECT countIf(data_uncompressed_bytes > 0) FROM system.data_skipping_indices WHERE database = currentDatabase()
    AND table = 't_sibling_marker_modify';
SELECT count() = 1 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_sibling_marker_modify WHERE c = '150'
    SETTINGS ignore_data_skipping_indices = 'idx_new') WHERE extract(explain, 'Granules: (\d+/\d+)') = '0/16';
SELECT count() = 1 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_sibling_marker_modify WHERE c = '150'
    SETTINGS ignore_data_skipping_indices = 'idx_old') WHERE extract(explain, 'Granules: (\d+/\d+)') = '0/16';
SELECT count() FROM t_sibling_marker_modify WHERE c = '150';
SELECT count() FROM t_sibling_marker_modify WHERE c = '150' SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_sibling_marker_modify WHERE c = '';
SELECT count() FROM t_sibling_marker_modify WHERE c = '' SETTINGS use_skip_indexes = 0;

SELECT '-- 34b. a sibling index rebuilt by CLEAR COLUMN of a marker-only column does not keep the column absent';
DROP TABLE IF EXISTS t_sibling_marker_cleared;
CREATE TABLE t_sibling_marker_cleared (k UInt64, d DateTime, c String TTL d + INTERVAL 1 SECOND,
    g UInt64, INDEX idx_old (c, g) TYPE set(100) GRANULARITY 1)
ENGINE = MergeTree ORDER BY k
SETTINGS index_granularity = 4, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
    ratio_of_defaults_for_sparse_serialization = 1.0,
    skip_empty_columns_on_insert = 1, serialization_info_version = 'with_missing_columns';
INSERT INTO t_sibling_marker_cleared SELECT number, '2000-01-01 00:00:00', toString(number * 3), 0 FROM numbers(64);
SELECT count() = 0 FROM system.parts_columns WHERE database = currentDatabase()
    AND table = 't_sibling_marker_cleared' AND active AND column = 'g';
ALTER TABLE t_sibling_marker_cleared MATERIALIZE TTL SETTINGS mutations_sync = 2, alter_sync = 2;
SELECT count() = 0 FROM system.parts_columns WHERE database = currentDatabase()
    AND table = 't_sibling_marker_cleared' AND active AND column = 'c';
SELECT count() = 0 FROM system.parts_columns WHERE database = currentDatabase()
    AND table = 't_sibling_marker_cleared' AND active AND column = 'g';
ALTER TABLE t_sibling_marker_cleared MODIFY COLUMN c REMOVE TTL SETTINGS alter_sync = 2;
ALTER TABLE t_sibling_marker_cleared ADD INDEX idx_new c TYPE set(100) GRANULARITY 1 SETTINGS alter_sync = 2;
-- Same batching as case 33: `CLEAR COLUMN` queues a mutation entry of its own, and only two pending
-- entries reach the part as one command set.
SYSTEM STOP MERGES t_sibling_marker_cleared;
ALTER TABLE t_sibling_marker_cleared CLEAR COLUMN g SETTINGS mutations_sync = 0, alter_sync = 0;
ALTER TABLE t_sibling_marker_cleared MATERIALIZE INDEX idx_new SETTINGS mutations_sync = 0, alter_sync = 0;
-- Both entries are pending together, so the executor applies them to the part as one command set.
SELECT count() = 2 FROM system.mutations WHERE database = currentDatabase()
    AND table = 't_sibling_marker_cleared' AND NOT is_done;
SYSTEM START MERGES t_sibling_marker_cleared;
ALTER TABLE t_sibling_marker_cleared UPDATE d = d WHERE 0 SETTINGS mutations_sync = 2, alter_sync = 2;
SYSTEM STOP MERGES t_sibling_marker_cleared;
SELECT count() > 0 FROM system.parts_columns WHERE database = currentDatabase()
    AND table = 't_sibling_marker_cleared' AND active AND column = 'c';
SELECT count() = 64 FROM t_sibling_marker_cleared WHERE g = 0;
-- Both indices hold files: `idx_old` is rebuilt over the cleared marker-only column rather than dropped.
SELECT countIf(data_uncompressed_bytes > 0) FROM system.data_skipping_indices WHERE database = currentDatabase()
    AND table = 't_sibling_marker_cleared';
SELECT count() = 1 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_sibling_marker_cleared WHERE c = '150'
    SETTINGS ignore_data_skipping_indices = 'idx_old') WHERE extract(explain, 'Granules: (\d+/\d+)') = '0/16';
SELECT count() FROM t_sibling_marker_cleared WHERE c = '150';
SELECT count() FROM t_sibling_marker_cleared WHERE c = '150' SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_sibling_marker_cleared WHERE c = '';
SELECT count() FROM t_sibling_marker_cleared WHERE c = '' SETTINGS use_skip_indexes = 0;

DROP TABLE t_sibling_marker_modify;
DROP TABLE t_sibling_marker_cleared;
