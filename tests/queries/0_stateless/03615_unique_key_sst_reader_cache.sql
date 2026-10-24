-- Tags: no-ordinary-database, no-async-insert, no-fasttest
-- The SST reader cache: a repeated dedup probe of the same part must reuse the
-- already-opened reader instead of reopening the SST (footer + index + filter).

SET enable_unique_key = 1;

DROP TABLE IF EXISTS uk_sst_reader_cache;
CREATE TABLE uk_sst_reader_cache (id UInt64, v String)
ENGINE = MergeTree
UNIQUE KEY (id)
ORDER BY (id)
SETTINGS min_rows_for_wide_part = 1, min_bytes_for_wide_part = 1,
         max_bytes_to_merge_at_max_space_in_pool = 1;

INSERT INTO uk_sst_reader_cache VALUES (1, 'a'), (2, 'b');

-- First probe of the part opens its SST reader (miss) and caches it.
SET log_comment = 'uk_sst_reader_cache_miss';
INSERT INTO uk_sst_reader_cache VALUES (1, 'x');

-- Second probe hits the cache.
SET log_comment = 'uk_sst_reader_cache_hit';
INSERT INTO uk_sst_reader_cache VALUES (2, 'y');

SYSTEM FLUSH LOGS query_log;

SELECT 'hit_observed';
SELECT ProfileEvents['UniqueKeySSTReaderCacheHits'] > 0
FROM system.query_log
WHERE log_comment = 'uk_sst_reader_cache_hit' AND type = 'QueryFinish' AND current_database = currentDatabase()
ORDER BY event_time DESC LIMIT 1;

SELECT 'rows';
SELECT id, v FROM uk_sst_reader_cache ORDER BY id;

DROP TABLE uk_sst_reader_cache;
