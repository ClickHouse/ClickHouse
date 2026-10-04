-- Tags: no-parallel-replicas
-- no-parallel-replicas: `EXPLAIN` output differs for parallel replicas, and `use_skip_indexes_on_data_read`
-- is not supported with parallel replicas.

DROP TABLE IF EXISTS t_skip_index_alter_nullable;

CREATE TABLE t_skip_index_alter_nullable
(
    id UInt64,
    value String,
    INDEX idx_value (value) TYPE set(0) GRANULARITY 1
)
ENGINE = MergeTree()
ORDER BY id
PARTITION BY id
SETTINGS add_minmax_index_for_numeric_columns = 0, min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0;

INSERT INTO t_skip_index_alter_nullable VALUES (1, '10'), (2, '20'), (3, '300');

-- Stop merges so the `ALTER MODIFY COLUMN` mutation stays pending and the old parts keep
-- their `String`-serialized set index data.
SYSTEM STOP MERGES t_skip_index_alter_nullable;

SET alter_sync = 0, mutations_sync = 0;
ALTER TABLE t_skip_index_alter_nullable MODIFY COLUMN value Nullable(UInt64);

-- With `apply_mutations_on_fly = 0` AND `apply_patch_parts = 0` the read snapshot omits the pending
-- `READ_COLUMN` alter mutation, so nothing in the snapshot disables `idx_value`. The old granules hold
-- `String` data and must be rejected by the stale-type check in `IMergeTreeIndex::getDeserializedFormat`;
-- decoding them as `Nullable(UInt64)` raises a `LOGICAL_ERROR` exception ("Sizes of nested column and
-- null map ... are not equal after deserialization").
-- `use_skip_indexes_on_data_read = 0` keeps this read in planning-time index analysis.
SELECT count()
FROM t_skip_index_alter_nullable
WHERE value = 300
SETTINGS apply_mutations_on_fly = 0, apply_patch_parts = 0, use_skip_indexes = 1, use_skip_indexes_on_data_read = 0, optimize_use_projections = 1, optimize_use_implicit_projections = 1, use_statistics_for_part_pruning = 0, use_query_condition_cache = 0, enable_analyzer = 1;

-- Same query with the on-fly apply flags at their defaults: must also work and give the same result.
SELECT count()
FROM t_skip_index_alter_nullable
WHERE value = 300
SETTINGS use_skip_indexes = 1, use_skip_indexes_on_data_read = 0, optimize_use_projections = 1, optimize_use_implicit_projections = 1, use_statistics_for_part_pruning = 0, use_query_condition_cache = 0, enable_analyzer = 1;

-- The direct skip-index read deserializes the same granules in `MergeTreeSkipIndexReader::read`
-- (via `MergeTreeIndexBulkGranulesSet::deserializeBinary`) instead of during planning, so pin it
-- under the same flags. `max_rows_to_read = 0` is required because the data-read phase disables
-- itself when `clickhouse-test` injects `read_overflow_mode = throw` with a row limit.
-- `use_skip_indexes = 1` is pinned explicitly: `force_data_skipping_indices` does not throw when
-- skip indexes are switched off globally, so without the pin this row would exercise nothing.
SELECT id
FROM t_skip_index_alter_nullable
WHERE value = 300
SETTINGS apply_mutations_on_fly = 0, apply_patch_parts = 0, use_skip_indexes = 1, force_data_skipping_indices = 'idx_value', use_skip_indexes_on_data_read = 1, secondary_indices_enable_bulk_filtering = 1, max_rows_to_read = 0, optimize_use_implicit_projections = 0, use_statistics_for_part_pruning = 0, use_query_condition_cache = 0, enable_analyzer = 1, log_comment = '05317_direct_read_stale';

-- Index analysis still consults `idx_value` on every old part but prunes nothing with it.
SELECT arrayStringConcat(groupArray(explain), '\n') ILIKE '%Name: idx_value%Granules: 3/3%' FROM (
    EXPLAIN indexes = 1
    SELECT id FROM t_skip_index_alter_nullable WHERE value = 300
    SETTINGS apply_mutations_on_fly = 0, apply_patch_parts = 0, use_skip_indexes = 1, use_skip_indexes_on_data_read = 0, optimize_use_implicit_projections = 0, use_statistics_for_part_pruning = 0);

-- The reads above must have run against all three old parts.
SELECT is_done, parts_to_do FROM system.mutations WHERE database = currentDatabase() AND table = 't_skip_index_alter_nullable';

-- A later mutation completes only after the pending one, so this waits for the `MODIFY COLUMN` too.
SYSTEM START MERGES t_skip_index_alter_nullable;
ALTER TABLE t_skip_index_alter_nullable MATERIALIZE INDEX idx_value SETTINGS mutations_sync = 2;
SELECT count() FROM system.mutations WHERE database = currentDatabase() AND table = 't_skip_index_alter_nullable' AND NOT is_done;

-- The rebuilt index prunes again.
SELECT arrayStringConcat(groupArray(explain), '\n') ILIKE '%Name: idx_value%Granules: 1/3%' FROM (
    EXPLAIN indexes = 1
    SELECT id FROM t_skip_index_alter_nullable WHERE value = 300
    SETTINGS apply_mutations_on_fly = 0, apply_patch_parts = 0, use_skip_indexes = 1, use_skip_indexes_on_data_read = 0, optimize_use_implicit_projections = 0, use_statistics_for_part_pruning = 0);

-- The same direct read on the rebuilt index. Planning defers filtering to the read-time pool and
-- reports every granule, so `SelectedMarks` dropping from 3 (stale index rejected) to 1 shows that
-- the read-time filter ran in both reads.
SELECT count() > 0 FROM (
    EXPLAIN ANALYZE indexes = 1
    SELECT id FROM t_skip_index_alter_nullable WHERE value = 300
    SETTINGS apply_mutations_on_fly = 0, apply_patch_parts = 0, use_skip_indexes = 1, force_data_skipping_indices = 'idx_value', use_skip_indexes_on_data_read = 1, secondary_indices_enable_bulk_filtering = 1, max_rows_to_read = 0, optimize_use_implicit_projections = 0, use_statistics_for_part_pruning = 0, use_query_condition_cache = 0)
WHERE explain ILIKE '%Parts: 3 | Granules: 3%';

SELECT id
FROM t_skip_index_alter_nullable
WHERE value = 300
SETTINGS apply_mutations_on_fly = 0, apply_patch_parts = 0, use_skip_indexes = 1, force_data_skipping_indices = 'idx_value', use_skip_indexes_on_data_read = 1, secondary_indices_enable_bulk_filtering = 1, max_rows_to_read = 0, optimize_use_implicit_projections = 0, use_statistics_for_part_pruning = 0, use_query_condition_cache = 0, enable_analyzer = 1, log_comment = '05317_direct_read_rebuilt';

SYSTEM FLUSH LOGS query_log;
SELECT log_comment, ProfileEvents['SelectedMarks']
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment IN ('05317_direct_read_stale', '05317_direct_read_rebuilt')
ORDER BY event_time_microseconds;

SELECT count()
FROM t_skip_index_alter_nullable
WHERE value = 300
SETTINGS apply_mutations_on_fly = 0, apply_patch_parts = 0, use_skip_indexes = 1, use_skip_indexes_on_data_read = 0, optimize_use_projections = 1, optimize_use_implicit_projections = 1, use_statistics_for_part_pruning = 0, use_query_condition_cache = 0, enable_analyzer = 1;

DROP TABLE t_skip_index_alter_nullable;
