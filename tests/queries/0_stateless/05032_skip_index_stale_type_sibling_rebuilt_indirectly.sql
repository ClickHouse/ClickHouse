-- Tags: no-random-merge-tree-settings, no-parallel-replicas
-- no-random-merge-tree-settings: every case pins index_granularity so the granule counts are stable.
-- no-parallel-replicas: EXPLAIN output differs for parallel replicas (an extra per-node Granules
-- block).
-- Cases 30-32c of the series started in 04165_skip_index_stale_type_after_alter and continued in
-- 04869_skip_index_stale_type_absent_column: the sibling index that decides whether the mutation may
-- record an absent column is rebuilt through something the commands do not name -- a column TTL, a
-- `MATERIALIZE TTL`, or a `MATERIALIZED` column. The series is split across files because one test
-- exceeded the flaky-check runtime limit under sanitizers; the original case numbering is kept.

SET mutations_sync = 0, alter_sync = 0;
-- Statistics part pruning is an independent mechanism that can drop a whole part before any index
-- is read, which would make these assertions measure something other than the skip index.
SET use_statistics_for_part_pruning = 0;

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

SELECT '-- 31. a sibling index rebuilt by MATERIALIZE TTL does not keep the column absent';
-- Unlike case 30, the command itself is MATERIALIZE TTL. Its column TTL target rewrites g and
-- therefore idx_old, while MATERIALIZE INDEX builds idx_new in the same mutation.
DROP TABLE IF EXISTS t_sibling_materialize_ttl_rebuilt;
CREATE TABLE t_sibling_materialize_ttl_rebuilt (k UInt64, d DateTime, c String TTL d + INTERVAL 1 SECOND,
    g String TTL d + INTERVAL 100 YEAR, INDEX idx_old (c, g) TYPE set(100) GRANULARITY 1)
ENGINE = MergeTree ORDER BY k
SETTINGS index_granularity = 4, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
INSERT INTO t_sibling_materialize_ttl_rebuilt SELECT number, '2000-01-01 00:00:00', toString(number * 3), toString(number) FROM numbers(64);
ALTER TABLE t_sibling_materialize_ttl_rebuilt MATERIALIZE TTL SETTINGS mutations_sync = 2, alter_sync = 2;
SELECT count() = 0 FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_sibling_materialize_ttl_rebuilt' AND active AND column = 'c';
ALTER TABLE t_sibling_materialize_ttl_rebuilt MODIFY COLUMN c REMOVE TTL SETTINGS alter_sync = 2;
ALTER TABLE t_sibling_materialize_ttl_rebuilt ADD INDEX idx_new c TYPE set(100) GRANULARITY 1 SETTINGS alter_sync = 2;
ALTER TABLE t_sibling_materialize_ttl_rebuilt MATERIALIZE TTL, MATERIALIZE INDEX idx_new
    SETTINGS mutations_sync = 2, alter_sync = 2;
SYSTEM STOP MERGES t_sibling_materialize_ttl_rebuilt;
SELECT count() > 0 FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_sibling_materialize_ttl_rebuilt' AND active AND column = 'c';
SELECT count() = 64 FROM t_sibling_materialize_ttl_rebuilt WHERE g != '';
SELECT countIf(data_uncompressed_bytes > 0) FROM system.data_skipping_indices WHERE database = currentDatabase() AND table = 't_sibling_materialize_ttl_rebuilt';
SELECT count() = 1 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_sibling_materialize_ttl_rebuilt WHERE c = '150'
    SETTINGS ignore_data_skipping_indices = 'idx_new') WHERE extract(explain, 'Granules: (\d+/\d+)') = '0/16';
SELECT count() = 1 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_sibling_materialize_ttl_rebuilt WHERE c = '150'
    SETTINGS ignore_data_skipping_indices = 'idx_old') WHERE extract(explain, 'Granules: (\d+/\d+)') = '0/16';
SELECT count() FROM t_sibling_materialize_ttl_rebuilt WHERE c = '150';
SELECT count() FROM t_sibling_materialize_ttl_rebuilt WHERE c = '150' SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_sibling_materialize_ttl_rebuilt WHERE c = '';
SELECT count() FROM t_sibling_materialize_ttl_rebuilt WHERE c = '' SETTINGS use_skip_indexes = 0;

SELECT '-- 32. a sibling index rebuilt through a MATERIALIZED column does not keep the column absent';
-- Updating e recomputes m, which idx_old reads along with the absent c. The mutation therefore
-- rebuilds idx_old through m even though it does not directly name an index column. Since both
-- indices are written from current data, c must be recorded for idx_new to be usable.
DROP TABLE IF EXISTS t_sibling_materialized_rebuilt;
CREATE TABLE t_sibling_materialized_rebuilt (
    k UInt64,
    d DateTime,
    c String TTL d + INTERVAL 1 SECOND,
    e UInt64,
    m UInt64 MATERIALIZED e * 2,
    INDEX idx_old (c, m) TYPE set(100) GRANULARITY 1)
ENGINE = MergeTree ORDER BY k
SETTINGS index_granularity = 4, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
INSERT INTO t_sibling_materialized_rebuilt SELECT number, '2000-01-01 00:00:00', toString(number * 3), number FROM numbers(64);
ALTER TABLE t_sibling_materialized_rebuilt MATERIALIZE TTL SETTINGS mutations_sync = 2, alter_sync = 2;
SELECT count() = 0 FROM system.parts_columns WHERE database = currentDatabase()
    AND table = 't_sibling_materialized_rebuilt' AND active AND column = 'c';
ALTER TABLE t_sibling_materialized_rebuilt MODIFY COLUMN c REMOVE TTL SETTINGS alter_sync = 2;
ALTER TABLE t_sibling_materialized_rebuilt ADD INDEX idx_new c TYPE set(100) GRANULARITY 1 SETTINGS alter_sync = 2;
ALTER TABLE t_sibling_materialized_rebuilt UPDATE e = e + 1 WHERE 1, MATERIALIZE INDEX idx_new
    SETTINGS mutations_sync = 2, alter_sync = 2;
SYSTEM STOP MERGES t_sibling_materialized_rebuilt;
SELECT count() > 0 FROM system.parts_columns WHERE database = currentDatabase()
    AND table = 't_sibling_materialized_rebuilt' AND active AND column = 'c';
SELECT countIf(data_uncompressed_bytes > 0) FROM system.data_skipping_indices WHERE database = currentDatabase()
    AND table = 't_sibling_materialized_rebuilt';
SELECT count() = 1 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_sibling_materialized_rebuilt WHERE c = '150'
    SETTINGS ignore_data_skipping_indices = 'idx_new') WHERE extract(explain, 'Granules: (\d+/\d+)') = '0/16';
SELECT count() = 1 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_sibling_materialized_rebuilt WHERE c = '150'
    SETTINGS ignore_data_skipping_indices = 'idx_old') WHERE extract(explain, 'Granules: (\d+/\d+)') = '0/16';
SELECT count() FROM t_sibling_materialized_rebuilt WHERE c = '150';
SELECT count() FROM t_sibling_materialized_rebuilt WHERE c = '150' SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_sibling_materialized_rebuilt WHERE c = '';
SELECT count() FROM t_sibling_materialized_rebuilt WHERE c = '' SETTINGS use_skip_indexes = 0;

SELECT '-- 32b. the MATERIALIZED column the sibling index reads may hop through an ALIAS';
-- Case 32 with one change: m reads the table ALIAS a_alias rather than e directly. An ALIAS is
-- computed on read and is not a source column, so the dependency analysis has to expand it exactly
-- as `MutationsInterpreter` does; resolving the name against the physical columns alone throws
-- `UNKNOWN_IDENTIFIER` and leaves the mutation unfinished.
DROP TABLE IF EXISTS t_sibling_materialized_alias_rebuilt;
CREATE TABLE t_sibling_materialized_alias_rebuilt (
    k UInt64,
    d DateTime,
    c String TTL d + INTERVAL 1 SECOND,
    e UInt64,
    a_alias UInt64 ALIAS e * 2,
    m UInt64 MATERIALIZED a_alias,
    INDEX idx_old (c, m) TYPE set(100) GRANULARITY 1)
ENGINE = MergeTree ORDER BY k
SETTINGS index_granularity = 4, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
INSERT INTO t_sibling_materialized_alias_rebuilt SELECT number, '2000-01-01 00:00:00', toString(number * 3), number FROM numbers(64);
ALTER TABLE t_sibling_materialized_alias_rebuilt MATERIALIZE TTL SETTINGS mutations_sync = 2, alter_sync = 2;
SELECT count() = 0 FROM system.parts_columns WHERE database = currentDatabase()
    AND table = 't_sibling_materialized_alias_rebuilt' AND active AND column = 'c';
ALTER TABLE t_sibling_materialized_alias_rebuilt MODIFY COLUMN c REMOVE TTL SETTINGS alter_sync = 2;
ALTER TABLE t_sibling_materialized_alias_rebuilt ADD INDEX idx_new c TYPE set(100) GRANULARITY 1 SETTINGS alter_sync = 2;
ALTER TABLE t_sibling_materialized_alias_rebuilt UPDATE e = e + 1 WHERE 1, MATERIALIZE INDEX idx_new
    SETTINGS mutations_sync = 2, alter_sync = 2;
SYSTEM STOP MERGES t_sibling_materialized_alias_rebuilt;
-- The mutation must have gone through on the first attempt: a throw here is retried after a restart,
-- so only the recorded failure reason tells the two apart.
SELECT countIf(latest_fail_reason != '') = 0 FROM system.mutations WHERE database = currentDatabase()
    AND table = 't_sibling_materialized_alias_rebuilt';
SELECT count() > 0 FROM system.parts_columns WHERE database = currentDatabase()
    AND table = 't_sibling_materialized_alias_rebuilt' AND active AND column = 'c';
SELECT countIf(data_uncompressed_bytes > 0) FROM system.data_skipping_indices WHERE database = currentDatabase()
    AND table = 't_sibling_materialized_alias_rebuilt';
SELECT count() = 1 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_sibling_materialized_alias_rebuilt WHERE c = '150'
    SETTINGS ignore_data_skipping_indices = 'idx_new') WHERE extract(explain, 'Granules: (\d+/\d+)') = '0/16';
SELECT count() = 1 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_sibling_materialized_alias_rebuilt WHERE c = '150'
    SETTINGS ignore_data_skipping_indices = 'idx_old') WHERE extract(explain, 'Granules: (\d+/\d+)') = '0/16';
SELECT count() FROM t_sibling_materialized_alias_rebuilt WHERE c = '150';
SELECT count() FROM t_sibling_materialized_alias_rebuilt WHERE c = '150' SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_sibling_materialized_alias_rebuilt WHERE c = '';
SELECT count() FROM t_sibling_materialized_alias_rebuilt WHERE c = '' SETTINGS use_skip_indexes = 0;

SELECT '-- 32c. a bare MATERIALIZE TTL does not rebuild a sibling index through a MATERIALIZED column';
-- Case 31 with idx_old reading a MATERIALIZED column instead of the TTL target itself.
-- `MutationsInterpreter::prepare` recomputes MATERIALIZED columns only from the columns the commands
-- name, and a bare MATERIALIZE TTL names none, so idx_old is carried over here rather than rebuilt.
DROP TABLE IF EXISTS t_sibling_materialized_ttl_only;
CREATE TABLE t_sibling_materialized_ttl_only (
    k UInt64,
    d DateTime,
    c String TTL d + INTERVAL 1 SECOND,
    g String TTL d + INTERVAL 100 YEAR,
    m String MATERIALIZED g,
    INDEX idx_old (c, m) TYPE set(100) GRANULARITY 1)
ENGINE = MergeTree ORDER BY k
SETTINGS index_granularity = 4, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
INSERT INTO t_sibling_materialized_ttl_only SELECT number, '2000-01-01 00:00:00', toString(number * 3), toString(number) FROM numbers(64);
ALTER TABLE t_sibling_materialized_ttl_only MATERIALIZE TTL SETTINGS mutations_sync = 2, alter_sync = 2;
SELECT count() = 0 FROM system.parts_columns WHERE database = currentDatabase()
    AND table = 't_sibling_materialized_ttl_only' AND active AND column = 'c';
ALTER TABLE t_sibling_materialized_ttl_only MODIFY COLUMN c REMOVE TTL SETTINGS alter_sync = 2;
SYSTEM STOP MERGES t_sibling_materialized_ttl_only;
ALTER TABLE t_sibling_materialized_ttl_only MODIFY COLUMN c Nullable(UInt64);
KILL MUTATION WHERE database = currentDatabase() AND table = 't_sibling_materialized_ttl_only' FORMAT Null;
ALTER TABLE t_sibling_materialized_ttl_only ADD INDEX idx_new c TYPE set(100) GRANULARITY 1 SETTINGS alter_sync = 2;
SYSTEM START MERGES t_sibling_materialized_ttl_only;
ALTER TABLE t_sibling_materialized_ttl_only MATERIALIZE TTL, MATERIALIZE INDEX idx_new
    SETTINGS mutations_sync = 2, alter_sync = 2;
SYSTEM STOP MERGES t_sibling_materialized_ttl_only;
-- Recording c at Nullable(UInt64) would let idx_old read granules built from its String values.
SELECT count() = 0 FROM system.parts_columns WHERE database = currentDatabase()
    AND table = 't_sibling_materialized_ttl_only' AND active AND column = 'c';
SELECT count() = 64 FROM t_sibling_materialized_ttl_only WHERE g != '';
SELECT countIf(data_uncompressed_bytes > 0) FROM system.data_skipping_indices WHERE database = currentDatabase()
    AND table = 't_sibling_materialized_ttl_only';
-- No granule is dropped anywhere in the plan: idx_old refuses instead of pruning stale granules.
SELECT count() = 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_sibling_materialized_ttl_only WHERE c = 150
    SETTINGS ignore_data_skipping_indices = 'idx_new') WHERE extract(explain, 'Granules: (\d+/\d+)') NOT IN ('', '16/16');
SELECT count() FROM t_sibling_materialized_ttl_only WHERE c = 150;
SELECT count() FROM t_sibling_materialized_ttl_only WHERE c = 150 SETTINGS use_skip_indexes = 0;

DROP TABLE t_sibling_ttl_rebuilt;
DROP TABLE t_sibling_materialize_ttl_rebuilt;
DROP TABLE t_sibling_materialized_rebuilt;
DROP TABLE t_sibling_materialized_alias_rebuilt;
DROP TABLE t_sibling_materialized_ttl_only;
