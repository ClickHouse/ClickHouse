-- Tags: no-fasttest, no-parallel, no-random-settings, no-random-merge-tree-settings, no-parallel-replicas, no-replicated-database
-- - no-fasttest -- requires S3
-- - no-parallel -- `SYSTEM DROP COLUMNS CACHE` is server-wide
-- - no-random-settings, no-random-merge-tree-settings -- the read method, the prefetch flag and
--   the task split have to be deterministic
-- - no-parallel-replicas -- another replica would do the reading, and the prefetching
-- - no-replicated-database -- the cache is per server

-- A read that the columns cache serves as a whole must not prefetch the streams of the range
-- first. `MergeTreePrefetchedReadPool` asks the reader to prefetch the beginning of a range
-- before the task reaches a reading thread, and on object storage a prefetch is a request over
-- the network - exactly the cost the cache exists to avoid, on the storage where it matters
-- most. `prefetchBeginOfRange` therefore probes the cache with the very function `readRows`
-- decides with, and skips the prefetch for a range that is going to be served from memory.

DROP TABLE IF EXISTS t_cc_remote_prefetch;

CREATE TABLE t_cc_remote_prefetch (c1 String, c2 String, c3 String)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS disk = 's3_no_cache', min_bytes_for_wide_part = 0, index_granularity = 8192;

INSERT INTO t_cc_remote_prefetch SELECT * FROM generateRandom() LIMIT 20000;

OPTIMIZE TABLE t_cc_remote_prefetch FINAL;

SYSTEM DROP COLUMNS CACHE;

-- `remote_filesystem_read_method = 'threadpool'` and `remote_filesystem_read_prefetch = 1` are
-- what make the prefetched read pool prefetch at all, and `max_threads = 1` keeps the part in
-- one task, so the cold and the hot read agree on the ranges the entries cover.
-- The cold read populates the cache and prefetches its range as usual.
SELECT * FROM t_cc_remote_prefetch FORMAT Null SETTINGS
    use_columns_cache = 1,
    enable_writes_to_columns_cache = 1,
    enable_reads_from_columns_cache = 1,
    allow_prefetched_read_pool_for_remote_filesystem = 1,
    remote_filesystem_read_method = 'threadpool',
    remote_filesystem_read_prefetch = 1,
    max_threads = 1,
    log_comment = '05154_cold';

-- The hot read is served from the cache from its first block to its last, so no stream of the
-- range is prefetched.
SELECT * FROM t_cc_remote_prefetch FORMAT Null SETTINGS
    use_columns_cache = 1,
    enable_writes_to_columns_cache = 1,
    enable_reads_from_columns_cache = 1,
    allow_prefetched_read_pool_for_remote_filesystem = 1,
    remote_filesystem_read_method = 'threadpool',
    remote_filesystem_read_prefetch = 1,
    max_threads = 1,
    log_comment = '05154_hot';

SYSTEM FLUSH LOGS query_log;

-- The cold read must have prefetched and missed, the hot one must have hit and not prefetched.
-- Asserting both directions keeps the test from passing when the cache is not engaged at all,
-- and from passing when nothing is ever prefetched on this disk.
SELECT
    log_comment,
    ProfileEvents['ColumnsCacheHits'] > 0 AS served_from_cache,
    ProfileEvents['RemoteFSPrefetches'] > 0 AS prefetched
FROM system.query_log
WHERE current_database = currentDatabase()
  AND log_comment IN ('05154_cold', '05154_hot')
  AND type = 'QueryFinish'
  AND event_time >= now() - INTERVAL 5 MINUTE
ORDER BY log_comment;

DROP TABLE t_cc_remote_prefetch;
