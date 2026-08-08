-- Tags: no-parallel-replicas
-- no-parallel-replicas: per-query SETTINGS toggling skip-index evaluation paths
-- must take effect on the executing replica, and the profile-event count below
-- must come from a single replica reading the whole part.

-- The bulk path reads and evaluates skip-index entries in chunks of `max_block_size`
-- granules. A tiny `max_block_size` therefore splits the index scan into many chunks;
-- the result and the number of evaluated granules must not depend on the chunk size,
-- and granules straddling a chunk boundary must not be re-evaluated or lost.

SET secondary_indices_enable_bulk_filtering = 1;
SET use_minmax_index_bulk_filtering = 1;
SET use_skip_indexes_on_data_read = 0;
SET use_statistics_for_part_pruning = 0;
-- The queries below are textually identical; without this, the second one reads the
-- first one's cached filter result and evaluates only the surviving granules.
SET use_query_condition_cache = 0;

DROP TABLE IF EXISTS t_minmax_chunks;

CREATE TABLE t_minmax_chunks
(
    k UInt64,
    v UInt64,
    INDEX idx_v v TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY k
SETTINGS index_granularity = 1, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;

-- One part, 100 data marks of 1 row each -> 100 skip-index granules.
INSERT INTO t_minmax_chunks SELECT number, number FROM numbers(100);

-- `max_block_size = 7` splits the 100-granule index scan into 15 chunks with the
-- matching granules [30, 60] spread across chunk boundaries.
SELECT count()
FROM t_minmax_chunks
WHERE v BETWEEN 30 AND 60
SETTINGS log_comment = '04818_chunked_bulk', max_block_size = 7;

-- Parity: one big chunk (default max_block_size) and the scalar path.
SELECT count()
FROM t_minmax_chunks
WHERE v BETWEEN 30 AND 60
SETTINGS log_comment = '04818_single_chunk_bulk';

SELECT count()
FROM t_minmax_chunks
WHERE v BETWEEN 30 AND 60
SETTINGS use_minmax_index_bulk_filtering = 0;

SYSTEM FLUSH LOGS query_log;

-- All 100 granules are examined exactly once, regardless of the chunk size.
SELECT log_comment, ProfileEvents['IndexBulkFilteringEvaluatedGranules']
FROM system.query_log
WHERE current_database = currentDatabase() AND log_comment IN ('04818_chunked_bulk', '04818_single_chunk_bulk')
  AND type = 'QueryFinish'
ORDER BY log_comment;

DROP TABLE t_minmax_chunks;
