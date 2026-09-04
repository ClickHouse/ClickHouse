-- Tags: no-parallel-replicas, no-replicated-database
-- no-parallel-replicas: profile events may differ with parallel replicas.
-- no-replicated-database: SYSTEM STOP MERGES is not replicated.
-- patch_parts_version is pinned because the patch ranges cache is used only in MergeOnKey mode (v2).
--
-- Several original parts with the same sort-key range are created, so that one patch part
-- covers all of them and the same patch ranges are requested for every part.
-- The first SELECT runs with one thread, which reads the original parts one by one, so the
-- reads for the first part populate the cache and the reads for the other parts are served
-- from it. The last two SELECTs stress the eviction and the entry admission limit, only
-- their results are checked. Note: the SELECT queries must not have comments directly
-- above them, because comments become part of the query text in query_log and would break
-- the LIKE patterns below.

SET allow_experimental_lightweight_update = 1;
SET apply_patch_parts = 1;

DROP TABLE IF EXISTS t_lwu_ranges_cache SYNC;

CREATE TABLE t_lwu_ranges_cache (id UInt64, v UInt64)
ENGINE = MergeTree ORDER BY id
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1, patch_parts_version = 'v2';

SYSTEM STOP MERGES t_lwu_ranges_cache;

INSERT INTO t_lwu_ranges_cache SELECT number, 0 FROM numbers(100000);
INSERT INTO t_lwu_ranges_cache SELECT number, 0 FROM numbers(100000);
INSERT INTO t_lwu_ranges_cache SELECT number, 0 FROM numbers(100000);
INSERT INTO t_lwu_ranges_cache SELECT number, 0 FROM numbers(100000);

UPDATE t_lwu_ranges_cache SET v = id + 1 WHERE id % 3 = 0;

SELECT count(), sum(v) FROM t_lwu_ranges_cache SETTINGS max_threads = 1, apply_patch_parts_ranges_cache_max_bytes = 1073741824;
SELECT count(), sum(v) FROM t_lwu_ranges_cache SETTINGS max_threads = 1, apply_patch_parts_ranges_cache_max_bytes = 0;
SELECT count(), sum(v) FROM t_lwu_ranges_cache SETTINGS max_threads = 4, apply_patch_parts_ranges_cache_max_bytes = 65536;
SELECT count(), sum(v) FROM t_lwu_ranges_cache SETTINGS max_threads = 4, apply_patch_parts_ranges_cache_max_bytes = 1;

SYSTEM FLUSH LOGS query_log;

SELECT
    ProfileEvents['PatchRangesCacheHits'] > 0,
    ProfileEvents['PatchRangesCacheMisses'] > 0
FROM system.query_log
WHERE event_date >= yesterday()
    AND current_database = currentDatabase()
    AND query LIKE 'SELECT count(), sum(v) FROM t_lwu_ranges_cache SETTINGS max_threads = 1%'
    AND type = 'QueryFinish'
ORDER BY event_time_microseconds;

DROP TABLE t_lwu_ranges_cache SYNC;
