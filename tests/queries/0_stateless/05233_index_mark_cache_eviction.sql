-- Tags: no-parallel
-- no-parallel: the index mark cache and its metrics are server-global.

-- Dropping a table releases its skip-index marks from the index mark cache, both for an
-- index packed into skp_idx.packed and for one written as its own files.

DROP TABLE IF EXISTS t_idx_mark_evict_packed SYNC;
DROP TABLE IF EXISTS t_idx_mark_evict_plain SYNC;

CREATE TABLE t_idx_mark_evict_packed (a UInt64, b UInt64, INDEX idx_b b TYPE minmax GRANULARITY 1)
ENGINE = MergeTree ORDER BY a
SETTINGS min_bytes_for_wide_part = 0, index_granularity = 1024,
         packed_skip_index_max_bytes = '1M', add_minmax_index_for_numeric_columns = 0;

CREATE TABLE t_idx_mark_evict_plain (a UInt64, b UInt64, INDEX idx_b b TYPE minmax GRANULARITY 1)
ENGINE = MergeTree ORDER BY a
SETTINGS min_bytes_for_wide_part = 0, index_granularity = 1024,
         packed_skip_index_max_bytes = 0, add_minmax_index_for_numeric_columns = 0;

INSERT INTO t_idx_mark_evict_packed SELECT number, number FROM numbers(20000);
INSERT INTO t_idx_mark_evict_plain SELECT number, number FROM numbers(20000);

SYSTEM DROP INDEX MARK CACHE;
SELECT 'empty', value FROM system.metrics WHERE metric = 'IndexMarkCacheFiles' ORDER BY value;

SYSTEM PREWARM MARK CACHE t_idx_mark_evict_packed;
SYSTEM PREWARM MARK CACHE t_idx_mark_evict_plain;
SELECT 'prewarmed', value FROM system.metrics WHERE metric = 'IndexMarkCacheFiles' ORDER BY value;

DROP TABLE t_idx_mark_evict_packed SYNC;
SELECT 'packed_dropped', value FROM system.metrics WHERE metric = 'IndexMarkCacheFiles' ORDER BY value;

DROP TABLE t_idx_mark_evict_plain SYNC;
SELECT 'both_dropped', value FROM system.metrics WHERE metric = 'IndexMarkCacheFiles' ORDER BY value;
DROP TABLE IF EXISTS t_idx_mark_evict_packed;
DROP TABLE IF EXISTS t_idx_mark_evict_plain;
