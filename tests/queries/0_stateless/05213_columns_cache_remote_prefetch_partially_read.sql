-- Tags: no-fasttest, no-parallel, no-random-settings, no-random-merge-tree-settings, no-parallel-replicas, no-replicated-database
-- - no-fasttest -- requires S3
-- - no-parallel -- `SYSTEM DROP COLUMNS CACHE` is server-wide
-- - no-random-settings, no-random-merge-tree-settings -- the read method, the prefetch flag and
--   the task split have to be deterministic
-- - no-parallel-replicas -- another replica would do the reading, and the prefetching
-- - no-replicated-database -- the cache is per server

-- The counterpart of `05154_columns_cache_remote_prefetch_skip`: a cache hit is not the same as a
-- read without IO, and only the second one may skip the prefetch.
--
-- A `Nested` member added by an `ALTER` after the part was written is a partially read column: the
-- cache cannot hold it, `findColumnsCacheEntriesForRange` therefore ignores it and reports a hit
-- as soon as the cacheable carrier column of the read is hot, and `readPartiallyReadColumnsWhileServing`
-- then reads its streams from the part while the rest of the range is served from memory. Skipping
-- the prefetch for such a range would leave exactly the repeated read that still goes to object
-- storage without any read-ahead, which is the cold-read latency the prefetch exists to hide.

DROP TABLE IF EXISTS t_cc_remote_partially_read;

CREATE TABLE t_cc_remote_partially_read
(
    tiny UInt8,
    id UInt64,
    `arr.id` Array(UInt64),
    `arr.nested` Array(Tuple(a String, b Float64))
)
ENGINE = MergeTree
ORDER BY id
SETTINGS
    disk = 's3_no_cache',
    share_nested_offsets = 1,
    min_bytes_for_wide_part = 0,
    min_rows_for_wide_part = 0,
    index_granularity = 8192,
    index_granularity_bytes = 0;

INSERT INTO t_cc_remote_partially_read
SELECT 0, number, [number], [(toString(number), number)] FROM numbers(20000);

-- Metadata-only: the part keeps no elements for `arr.nested`, only the shared `arr` offsets.
ALTER TABLE t_cc_remote_partially_read DROP COLUMN `arr.nested`;
ALTER TABLE t_cc_remote_partially_read ADD COLUMN `arr.nested` Array(Tuple(a String, b Float64));

OPTIMIZE TABLE t_cc_remote_partially_read FINAL;

SYSTEM DROP COLUMNS CACHE;

-- The read asks for nothing but the re-added member, so `injectRequiredColumns` adds the smallest
-- column of the part to carry the row count - and that column is the one the cache has an entry
-- for, which is what turns the second read into a hit.
SELECT sum(length(`arr.nested`.b)) FROM t_cc_remote_partially_read SETTINGS
    use_columns_cache = 1,
    enable_writes_to_columns_cache = 1,
    enable_reads_from_columns_cache = 1,
    allow_prefetched_read_pool_for_remote_filesystem = 1,
    remote_filesystem_read_method = 'threadpool',
    remote_filesystem_read_prefetch = 1,
    max_threads = 1,
    log_comment = '05213_cold';

-- The hot read is served from the cache, and still reads `arr.nested` from the part - so its range
-- must be prefetched all the same.
SELECT sum(length(`arr.nested`.b)) FROM t_cc_remote_partially_read SETTINGS
    use_columns_cache = 1,
    enable_writes_to_columns_cache = 1,
    enable_reads_from_columns_cache = 1,
    allow_prefetched_read_pool_for_remote_filesystem = 1,
    remote_filesystem_read_method = 'threadpool',
    remote_filesystem_read_prefetch = 1,
    max_threads = 1,
    log_comment = '05213_hot';

SYSTEM FLUSH LOGS query_log;

-- Requiring the hit of the second read keeps the test from passing on a build where the cache never
-- engages, which is the other way the prefetch could come out non-zero.
SELECT
    log_comment,
    ProfileEvents['ColumnsCacheHits'] > 0 AS served_from_cache,
    ProfileEvents['RemoteFSPrefetches'] > 0 AS prefetched
FROM system.query_log
WHERE current_database = currentDatabase()
  AND log_comment IN ('05213_cold', '05213_hot')
  AND type = 'QueryFinish'
  AND event_time >= now() - INTERVAL 5 MINUTE
ORDER BY log_comment;

DROP TABLE t_cc_remote_partially_read;
