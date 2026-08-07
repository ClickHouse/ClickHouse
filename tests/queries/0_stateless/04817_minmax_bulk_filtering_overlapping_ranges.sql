-- Tags: no-parallel-replicas
-- no-parallel-replicas: per-query SETTINGS toggling skip-index evaluation paths
-- must take effect on the executing replica, and the profile-event count below
-- must come from a single replica reading the whole part.

-- One skip-index granule can cover several PK-pruned mark ranges (GRANULARITY > 1
-- with a fragmented primary-key selection). The scalar path deduplicates that case
-- via `last_index_mark`; the bulk path must merge the overlapping index ranges before
-- chunking, so each skip-index granule is deserialized and evaluated exactly once and
-- `IndexBulkFilteringEvaluatedGranules` counts unique granules, not repetitions.

SET secondary_indices_enable_bulk_filtering = 1;
SET use_minmax_index_bulk_filtering = 1;
SET use_skip_indexes_on_data_read = 0;
SET use_statistics_for_part_pruning = 0;
-- Pin the seek-merge thresholds: the PK-pruned ranges must stay fragmented (not be
-- merged into one contiguous range) for the overlap case to be exercised.
SET merge_tree_min_rows_for_seek = 0;
SET merge_tree_min_bytes_for_seek = 0;

DROP TABLE IF EXISTS t_minmax_overlap;

CREATE TABLE t_minmax_overlap
(
    k UInt64,
    v UInt64,
    INDEX idx_v v TYPE minmax GRANULARITY 8
)
ENGINE = MergeTree
ORDER BY k
SETTINGS index_granularity = 1, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;

-- One part, 32 data marks of 1 row each -> 4 skip-index granules of 8 marks.
INSERT INTO t_minmax_overlap SELECT number, number FROM numbers(32);

-- The PK prunes to fragmented ranges [0,1), [3,4), [5,6) - three ranges inside the
-- same skip-index granule 0 - plus [17,18) in granule 2. The predicate on `v` passes
-- everywhere, so the index prunes nothing and every touched granule is evaluated.
SELECT count()
FROM t_minmax_overlap
WHERE k IN (0, 3, 5, 17) AND v < 1000
SETTINGS log_comment = '04817_overlap_bulk';

-- Parity: the scalar path must return the same result.
SELECT count()
FROM t_minmax_overlap
WHERE k IN (0, 3, 5, 17) AND v < 1000
SETTINGS log_comment = '04817_overlap_scalar', use_minmax_index_bulk_filtering = 0;

SYSTEM FLUSH LOGS query_log;

-- Granules 0 and 2 are touched: exactly 2 unique granules must be evaluated, even
-- though granule 0 covers three of the PK-pruned ranges (a duplicate-re-evaluation
-- bug would count 4).
SELECT 'unique granules evaluated',
    ProfileEvents['IndexBulkFilteringEvaluatedGranules']
FROM system.query_log
WHERE current_database = currentDatabase() AND log_comment = '04817_overlap_bulk'
  AND type = 'QueryFinish'
ORDER BY event_time_microseconds DESC LIMIT 1;

DROP TABLE t_minmax_overlap;
