-- Tags: no-random-merge-tree-settings, no-parallel-replicas
-- no-random-merge-tree-settings: every case pins index_granularity so the granule counts are stable.
-- no-parallel-replicas: EXPLAIN output differs for parallel replicas (an extra per-node Granules
-- block).
-- Cases 33-35 of the series started in 04165_skip_index_stale_type_after_alter and continued in
-- 04869_skip_index_stale_type_absent_column: the sibling index that decides whether a mutation may
-- record an absent column is itself rebuilt or dropped by the same mutation, through the
-- `MODIFY COLUMN`, `CLEAR COLUMN` and TTL lifecycle that `MutationsInterpreter` applies. The series
-- is split across files because one test exceeded the flaky-check runtime limit under sanitizers;
-- the original case numbering is kept.

SET mutations_sync = 0, alter_sync = 0;
-- Statistics part pruning is an independent mechanism that can drop a whole part before any index
-- is read, which would make these assertions measure something other than the skip index.
SET use_statistics_for_part_pruning = 0;

SELECT '-- 33. a sibling index rebuilt by MODIFY COLUMN does not keep the column absent';
-- `MutationsInterpreter` rebuilds every index over a column whose type the mutation changes, so
-- `idx_old` does not carry its granules over and must not keep the absent `c` out of the part.
DROP TABLE IF EXISTS t_sibling_modify_rebuilt;
CREATE TABLE t_sibling_modify_rebuilt (k UInt64, d DateTime, c String TTL d + INTERVAL 1 SECOND,
    g String, INDEX idx_old (c, g) TYPE set(100) GRANULARITY 1)
ENGINE = MergeTree ORDER BY k
SETTINGS index_granularity = 4, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
INSERT INTO t_sibling_modify_rebuilt SELECT number, '2000-01-01 00:00:00', toString(number * 3), toString(number) FROM numbers(64);
ALTER TABLE t_sibling_modify_rebuilt MATERIALIZE TTL SETTINGS mutations_sync = 2, alter_sync = 2;
SELECT count() = 0 FROM system.parts_columns WHERE database = currentDatabase()
    AND table = 't_sibling_modify_rebuilt' AND active AND column = 'c';
ALTER TABLE t_sibling_modify_rebuilt MODIFY COLUMN c REMOVE TTL SETTINGS alter_sync = 2;
ALTER TABLE t_sibling_modify_rebuilt ADD INDEX idx_new c TYPE set(100) GRANULARITY 1 SETTINGS alter_sync = 2;
-- `MODIFY COLUMN` is a metadata alter, so it gets a mutation entry of its own rather than sharing
-- one with `MATERIALIZE INDEX`. Submitting both while the background executor is stopped makes the
-- selector squash them into a single command set, which is the shape under test; the trailing no-op
-- mutation is only there to wait, since a later mutation completes after the earlier ones.
SYSTEM STOP MERGES t_sibling_modify_rebuilt;
ALTER TABLE t_sibling_modify_rebuilt MODIFY COLUMN g UInt64 SETTINGS mutations_sync = 0, alter_sync = 0;
ALTER TABLE t_sibling_modify_rebuilt MATERIALIZE INDEX idx_new SETTINGS mutations_sync = 0, alter_sync = 0;
SYSTEM START MERGES t_sibling_modify_rebuilt;
ALTER TABLE t_sibling_modify_rebuilt UPDATE d = d WHERE 0 SETTINGS mutations_sync = 2, alter_sync = 2;
SYSTEM STOP MERGES t_sibling_modify_rebuilt;
SELECT count() > 0 FROM system.parts_columns WHERE database = currentDatabase()
    AND table = 't_sibling_modify_rebuilt' AND active AND column = 'c';
-- `g` keeps its values at the new type, so `idx_old` was rebuilt from data rather than emptied.
SELECT count() = 63 FROM t_sibling_modify_rebuilt WHERE g != 0;
SELECT countIf(data_uncompressed_bytes > 0) FROM system.data_skipping_indices WHERE database = currentDatabase()
    AND table = 't_sibling_modify_rebuilt';
SELECT count() = 1 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_sibling_modify_rebuilt WHERE c = '150'
    SETTINGS ignore_data_skipping_indices = 'idx_new') WHERE extract(explain, 'Granules: (\d+/\d+)') = '0/16';
SELECT count() = 1 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_sibling_modify_rebuilt WHERE c = '150'
    SETTINGS ignore_data_skipping_indices = 'idx_old') WHERE extract(explain, 'Granules: (\d+/\d+)') = '0/16';
SELECT count() FROM t_sibling_modify_rebuilt WHERE c = '150';
SELECT count() FROM t_sibling_modify_rebuilt WHERE c = '150' SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_sibling_modify_rebuilt WHERE c = '';
SELECT count() FROM t_sibling_modify_rebuilt WHERE c = '' SETTINGS use_skip_indexes = 0;

SELECT '-- 34. a sibling index rebuilt by CLEAR COLUMN does not keep the column absent';
-- Clearing a column rebuilds every index over it from that column's default, so `idx_old` carries
-- no granules over and cannot constrain what the part records.
DROP TABLE IF EXISTS t_sibling_cleared;
CREATE TABLE t_sibling_cleared (k UInt64, d DateTime, c String TTL d + INTERVAL 1 SECOND,
    g String, INDEX idx_old (c, g) TYPE set(100) GRANULARITY 1)
ENGINE = MergeTree ORDER BY k
SETTINGS index_granularity = 4, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
INSERT INTO t_sibling_cleared SELECT number, '2000-01-01 00:00:00', toString(number * 3), toString(number) FROM numbers(64);
ALTER TABLE t_sibling_cleared MATERIALIZE TTL SETTINGS mutations_sync = 2, alter_sync = 2;
SELECT count() = 0 FROM system.parts_columns WHERE database = currentDatabase()
    AND table = 't_sibling_cleared' AND active AND column = 'c';
ALTER TABLE t_sibling_cleared MODIFY COLUMN c REMOVE TTL SETTINGS alter_sync = 2;
ALTER TABLE t_sibling_cleared ADD INDEX idx_new c TYPE set(100) GRANULARITY 1 SETTINGS alter_sync = 2;
ALTER TABLE t_sibling_cleared CLEAR COLUMN g, MATERIALIZE INDEX idx_new
    SETTINGS mutations_sync = 2, alter_sync = 2;
SYSTEM STOP MERGES t_sibling_cleared;
SELECT count() > 0 FROM system.parts_columns WHERE database = currentDatabase()
    AND table = 't_sibling_cleared' AND active AND column = 'c';
SELECT count() = 64 FROM t_sibling_cleared WHERE g = '';
-- Both indices hold files: `idx_old` is rebuilt over the cleared column rather than dropped.
SELECT countIf(data_uncompressed_bytes > 0) FROM system.data_skipping_indices WHERE database = currentDatabase()
    AND table = 't_sibling_cleared';
SELECT count() = 1 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_sibling_cleared WHERE c = '150'
    SETTINGS ignore_data_skipping_indices = 'idx_old') WHERE extract(explain, 'Granules: (\d+/\d+)') = '0/16';
SELECT count() FROM t_sibling_cleared WHERE c = '150';
SELECT count() FROM t_sibling_cleared WHERE c = '150' SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_sibling_cleared WHERE c = '';
SELECT count() FROM t_sibling_cleared WHERE c = '' SETTINGS use_skip_indexes = 0;

SELECT '-- 35. materialize_ttl_recalculate_only is ignored next to an UPDATE, and so is it here';
-- `MutationsInterpreter::prepare` forces `materialize_ttl_recalculate_only` off as soon as the same
-- command set updates rows, so `MATERIALIZE TTL` rewrites `g` and rebuilds `idx_old` over it. Reading
-- the naked setting instead would classify `idx_old` as carried over and leave `idx_new` inert.
DROP TABLE IF EXISTS t_sibling_ttl_recalculate_only;
CREATE TABLE t_sibling_ttl_recalculate_only (k UInt64, d DateTime, c String TTL d + INTERVAL 1 SECOND,
    g String TTL d + INTERVAL 100 YEAR, e UInt64, INDEX idx_old (c, g) TYPE set(100) GRANULARITY 1)
ENGINE = MergeTree ORDER BY k
SETTINGS index_granularity = 4, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
INSERT INTO t_sibling_ttl_recalculate_only SELECT number, '2000-01-01 00:00:00', toString(number * 3), toString(number), number FROM numbers(64);
ALTER TABLE t_sibling_ttl_recalculate_only MATERIALIZE TTL SETTINGS mutations_sync = 2, alter_sync = 2;
SELECT count() = 0 FROM system.parts_columns WHERE database = currentDatabase()
    AND table = 't_sibling_ttl_recalculate_only' AND active AND column = 'c';
ALTER TABLE t_sibling_ttl_recalculate_only MODIFY COLUMN c REMOVE TTL SETTINGS alter_sync = 2;
ALTER TABLE t_sibling_ttl_recalculate_only ADD INDEX idx_new c TYPE set(100) GRANULARITY 1 SETTINGS alter_sync = 2;
ALTER TABLE t_sibling_ttl_recalculate_only MODIFY SETTING materialize_ttl_recalculate_only = 1;
ALTER TABLE t_sibling_ttl_recalculate_only UPDATE e = e + 1 WHERE 1, MATERIALIZE TTL, MATERIALIZE INDEX idx_new
    SETTINGS mutations_sync = 2, alter_sync = 2;
SYSTEM STOP MERGES t_sibling_ttl_recalculate_only;
SELECT count() > 0 FROM system.parts_columns WHERE database = currentDatabase()
    AND table = 't_sibling_ttl_recalculate_only' AND active AND column = 'c';
SELECT count() = 64 FROM t_sibling_ttl_recalculate_only WHERE g != '';
SELECT countIf(data_uncompressed_bytes > 0) FROM system.data_skipping_indices WHERE database = currentDatabase()
    AND table = 't_sibling_ttl_recalculate_only';
SELECT count() = 1 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_sibling_ttl_recalculate_only WHERE c = '150'
    SETTINGS ignore_data_skipping_indices = 'idx_new') WHERE extract(explain, 'Granules: (\d+/\d+)') = '0/16';
SELECT count() = 1 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_sibling_ttl_recalculate_only WHERE c = '150'
    SETTINGS ignore_data_skipping_indices = 'idx_old') WHERE extract(explain, 'Granules: (\d+/\d+)') = '0/16';
SELECT count() FROM t_sibling_ttl_recalculate_only WHERE c = '150';
SELECT count() FROM t_sibling_ttl_recalculate_only WHERE c = '150' SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_sibling_ttl_recalculate_only WHERE c = '';
SELECT count() FROM t_sibling_ttl_recalculate_only WHERE c = '' SETTINGS use_skip_indexes = 0;

DROP TABLE t_sibling_modify_rebuilt;
DROP TABLE t_sibling_cleared;
DROP TABLE t_sibling_ttl_recalculate_only;
