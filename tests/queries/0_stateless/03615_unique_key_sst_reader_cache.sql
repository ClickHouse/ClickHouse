-- Tags: no-ordinary-database, no-async-insert, no-fasttest, no-random-settings, no-s3-storage
-- The SST reader cache: a repeated dedup probe of the same part must reuse the
-- already-opened reader instead of reopening the SST (footer + index + filter).
-- A query-level read throttler bypasses the cache, and so does a user-level one
-- on remote disks, so pin a local disk and drop the query-level limits.
-- no-random-settings: randomized read toggles (`enable_filesystem_cache`,
-- `use_page_cache_*`, ...) make the probe's settings differ from the shared
-- server-level settings, and such a probe bypasses the cache by design.
-- no-s3-storage: an S3-backed `default` policy makes the part remote, where the
-- test profile's user-level network throttler bypasses the cache too.

SET enable_unique_key = 1;
SET max_local_read_bandwidth = 0, max_remote_read_network_bandwidth = 0;

DROP TABLE IF EXISTS uk_sst_reader_cache;
CREATE TABLE uk_sst_reader_cache (id UInt64, v String)
ENGINE = MergeTree
UNIQUE KEY (id)
ORDER BY (id)
SETTINGS min_rows_for_wide_part = 1, min_bytes_for_wide_part = 1,
         max_bytes_to_merge_at_max_space_in_pool = 1, storage_policy = 'default';

INSERT INTO uk_sst_reader_cache VALUES (1, 'a'), (2, 'b');

-- First probe of the part opens its SST reader (miss) and caches it.
SET log_comment = 'uk_sst_reader_cache_miss';
INSERT INTO uk_sst_reader_cache VALUES (1, 'x');

-- Second probe hits the cache.
SET log_comment = 'uk_sst_reader_cache_hit';
INSERT INTO uk_sst_reader_cache VALUES (2, 'y');

-- A query-level read limit bypasses the cache: opened directly, neither hit nor miss.
SET log_comment = 'uk_sst_reader_cache_throttled';
INSERT INTO uk_sst_reader_cache SETTINGS max_local_read_bandwidth = 1000000000 VALUES (1, 'z');

SYSTEM FLUSH LOGS query_log;

SELECT 'hit_observed';
SELECT ProfileEvents['UniqueKeySSTReaderCacheHits'] > 0
FROM system.query_log
WHERE log_comment = 'uk_sst_reader_cache_hit' AND type = 'QueryFinish' AND current_database = currentDatabase()
ORDER BY event_time DESC LIMIT 1;

SELECT 'throttled_bypasses';
SELECT ProfileEvents['UniqueKeySSTReaderCacheHits'] + ProfileEvents['UniqueKeySSTReaderCacheMisses'] = 0
FROM system.query_log
WHERE log_comment = 'uk_sst_reader_cache_throttled' AND type = 'QueryFinish' AND current_database = currentDatabase()
ORDER BY event_time DESC LIMIT 1;

SELECT 'rows';
SELECT id, v FROM uk_sst_reader_cache ORDER BY id;

DROP TABLE uk_sst_reader_cache;
