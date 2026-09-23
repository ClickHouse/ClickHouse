-- Tags: no-random-merge-tree-settings, no-parallel-replicas
-- no-random-merge-tree-settings: the case pins index_granularity so the granule counts are stable,
-- and relies on skip_empty_columns_on_insert to leave a column marker-only.
-- no-parallel-replicas: EXPLAIN output differs for parallel replicas (an extra per-node Granules
-- block).
-- Case 32e of the series continued in 05032_skip_index_stale_type_sibling_rebuilt_indirectly: the
-- sibling index is rebuilt through a `MATERIALIZED` column the part holds only as a missing-column
-- marker (`skip_empty_columns_on_insert`). An `INSERT` never leaves an expression column
-- marker-only, so `m` is an ordinary column at `INSERT` and gains its `MATERIALIZED` default later.
-- `MutationsInterpreter::prepare` recomputes every physical `MATERIALIZED` column over an updated
-- one, whether the part stores it or not, and rebuilds the indices over it; the rebuilt-index
-- predictor in `splitAndModifyMutationCommands` must take the same decision, or it keeps the absent
-- carrier `c` out of the part and leaves the freshly materialized `idx_new` inert.

SET mutations_sync = 0, alter_sync = 0;
-- Statistics part pruning is an independent mechanism that can drop a whole part before any index
-- is read, which would make these assertions measure something other than the skip index.
SET use_statistics_for_part_pruning = 0;

SELECT '-- 32e. a sibling index rebuilt through a marker-only MATERIALIZED column does not keep the column absent';
DROP TABLE IF EXISTS t_sibling_marker_materialized;
CREATE TABLE t_sibling_marker_materialized (k UInt64, d DateTime, c String TTL d + INTERVAL 1 SECOND,
    e UInt64, m UInt64, INDEX idx_old (c, m) TYPE set(100) GRANULARITY 1)
ENGINE = MergeTree ORDER BY k
SETTINGS index_granularity = 4, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
    ratio_of_defaults_for_sparse_serialization = 1.0,
    skip_empty_columns_on_insert = 1, serialization_info_version = 'with_missing_columns';
-- `m` is all type-default, so the part records it as a marker only.
INSERT INTO t_sibling_marker_materialized SELECT number, '2000-01-01 00:00:00', toString(number * 3), number + 1, 0 FROM numbers(64);
SELECT count() = 0 FROM system.parts_columns WHERE database = currentDatabase()
    AND table = 't_sibling_marker_materialized' AND active AND column = 'm';
ALTER TABLE t_sibling_marker_materialized MATERIALIZE TTL SETTINGS mutations_sync = 2, alter_sync = 2;
SELECT count() = 0 FROM system.parts_columns WHERE database = currentDatabase()
    AND table = 't_sibling_marker_materialized' AND active AND column = 'c';
-- The marker survives the mutation.
SELECT count() = 0 FROM system.parts_columns WHERE database = currentDatabase()
    AND table = 't_sibling_marker_materialized' AND active AND column = 'm';
ALTER TABLE t_sibling_marker_materialized MODIFY COLUMN m UInt64 MATERIALIZED e * 0 SETTINGS alter_sync = 2;
-- Adding the default rewrites no data, so `m` is still marker-only when `e` is updated below.
SELECT count() = 0 FROM system.parts_columns WHERE database = currentDatabase()
    AND table = 't_sibling_marker_materialized' AND active AND column = 'm';
ALTER TABLE t_sibling_marker_materialized MODIFY COLUMN c REMOVE TTL SETTINGS alter_sync = 2;
ALTER TABLE t_sibling_marker_materialized ADD INDEX idx_new c TYPE set(100) GRANULARITY 1 SETTINGS alter_sync = 2;
ALTER TABLE t_sibling_marker_materialized UPDATE e = e + 1 WHERE 1, MATERIALIZE INDEX idx_new
    SETTINGS mutations_sync = 2, alter_sync = 2;
SYSTEM STOP MERGES t_sibling_marker_materialized;
-- Both commands share one mutation id, so the pipeline saw them as one command set.
SELECT uniqExact(mutation_id) = 1 FROM system.mutations WHERE database = currentDatabase()
    AND table = 't_sibling_marker_materialized' AND command LIKE '%idx_new%';
SELECT count() > 0 FROM system.parts_columns WHERE database = currentDatabase()
    AND table = 't_sibling_marker_materialized' AND active AND column = 'c';
SELECT count() = 64 FROM t_sibling_marker_materialized WHERE e = k + 2 AND m = 0;
-- Both indices hold files: a materialization that silently did nothing cannot pass.
SELECT countIf(data_uncompressed_bytes > 0) FROM system.data_skipping_indices WHERE database = currentDatabase()
    AND table = 't_sibling_marker_materialized';
-- Both were built from current data, so both must prune, and no row holds '150' after the expiry:
-- 0/16 for each, where 16/16 would be the refusal this case must not see.
SELECT count() = 1 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_sibling_marker_materialized WHERE c = '150'
    SETTINGS ignore_data_skipping_indices = 'idx_new') WHERE extract(explain, 'Granules: (\d+/\d+)') = '0/16';
SELECT count() = 1 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_sibling_marker_materialized WHERE c = '150'
    SETTINGS ignore_data_skipping_indices = 'idx_old') WHERE extract(explain, 'Granules: (\d+/\d+)') = '0/16';
SELECT count() FROM t_sibling_marker_materialized WHERE c = '150';
SELECT count() FROM t_sibling_marker_materialized WHERE c = '150' SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_sibling_marker_materialized WHERE c = '';
SELECT count() FROM t_sibling_marker_materialized WHERE c = '' SETTINGS use_skip_indexes = 0;

DROP TABLE t_sibling_marker_materialized;
