-- After dropping and re-adding a same-named index with a different definition, the not-yet-mutated part still
-- carries the index file of the old definition. Granules cached under the old definition must not be served to a
-- query that uses the new definition: the cache key includes a hash of the index definition, so the second query
-- below must miss the cache even though the part path, checksum and index name are unchanged.

SET parallel_replicas_local_plan = 1;
SET max_threads = 1;
SET use_skip_indexes_on_data_read = 0;
-- `ALTER ... DROP INDEX` waits for its file-removing mutation when `alter_sync` > 0, and the mutation is
-- blocked by `SYSTEM STOP MERGES` below.
SET alter_sync = 0;

DROP TABLE IF EXISTS tab;

CREATE TABLE tab
(
    id UInt64,
    s String,
    INDEX idx_tokens s TYPE tokenbf_v1(512, 3, 0) GRANULARITY 1
)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 16, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;

INSERT INTO tab SELECT number, concat('token', toString(number % 997), ' text') FROM numbers(4800);

-- Keeps the mutation of DROP INDEX from rewriting the part, so the old index file stays in the active part.
SYSTEM STOP MERGES tab;

SELECT count() FROM tab WHERE hasToken(s, 'token5');

-- Recreate the index with the same name and filter size but a different seed: the granule bytes deserialize
-- identically, only the definition differs. The result of the query after the ALTER is not checked (reading the
-- old file under the new definition is wrong with or without the cache, see the type-compatibility comment at
-- IMergeTreeIndex::getDeserializedFormat) - only the cache behavior is, which is why it outputs FORMAT Null.
ALTER TABLE tab DROP INDEX idx_tokens;
ALTER TABLE tab ADD INDEX idx_tokens s TYPE tokenbf_v1(512, 3, 1) GRANULARITY 1;
SELECT count() FROM tab WHERE hasToken(s, 'token5') FORMAT Null;

SYSTEM START MERGES tab;

SYSTEM FLUSH LOGS query_log;

SELECT
    ProfileEvents['SkippingIndexCacheMisses'] > 0 AS has_misses,
    ProfileEvents['SkippingIndexCacheHits'] AS hits
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600 AND current_database = currentDatabase() AND type = 'QueryFinish'
    AND query LIKE 'SELECT count() FROM tab WHERE hasToken%'
ORDER BY event_time_microseconds;

DROP TABLE tab;
