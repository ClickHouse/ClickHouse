-- Tags: no-parallel
-- no-parallel: looks at server-wide cache, which other tests may clear

-- The table has 300 index granules for GRANULARITY 1 => 3 blocks of the cache (128 + 128 + 44), 75 index granules for GRANULARITY 4.
-- Each query is run twice: the first run populates the cache, the second run must be served from it.
-- The queries with primary key pruning touch only a part of a block of granules, including the last (partial) block.
-- The queries with use_skip_indexes = 0 check that the results do not depend on the cache.

SET parallel_replicas_local_plan = 1;
SET max_threads = 1;
SET use_skip_indexes_on_data_read = 0;

DROP TABLE IF EXISTS tab;

CREATE TABLE tab
(
    id UInt64,
    key UInt64,
    s String,
    INDEX idx_key key TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_key_coarse key TYPE bloom_filter(0.01) GRANULARITY 4,
    INDEX idx_tokens s TYPE tokenbf_v1(512, 3, 0) GRANULARITY 1,
    INDEX idx_ngrams s TYPE ngrambf_v1(3, 512, 3, 0) GRANULARITY 1
)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 16, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;

INSERT INTO tab SELECT number, number * 7919 % 1000, concat('token', toString(number % 997), ' text') FROM numbers(4800);

SYSTEM DROP SKIPPING INDEX CACHE;

SELECT count() FROM tab WHERE key = 123;
SELECT count() FROM tab WHERE key = 123;
SELECT count() FROM tab WHERE hasToken(s, 'token5');
SELECT count() FROM tab WHERE hasToken(s, 'token5');
SELECT count() FROM tab WHERE s LIKE '%ken50 %';
SELECT count() FROM tab WHERE s LIKE '%ken50 %';

SELECT count() FROM tab WHERE id BETWEEN 2000 AND 2100 AND key = 123;
SELECT count() FROM tab WHERE id BETWEEN 4600 AND 4799 AND key = 123;

SELECT count() FROM tab WHERE key = 123 SETTINGS use_skip_indexes = 0;
SELECT count() FROM tab WHERE hasToken(s, 'token5') SETTINGS use_skip_indexes = 0;
SELECT count() FROM tab WHERE s LIKE '%ken50 %' SETTINGS use_skip_indexes = 0;
SELECT count() FROM tab WHERE id BETWEEN 2000 AND 2100 AND key = 123 SETTINGS use_skip_indexes = 0;
SELECT count() FROM tab WHERE id BETWEEN 4600 AND 4799 AND key = 123 SETTINGS use_skip_indexes = 0;

SYSTEM FLUSH LOGS query_log;

SELECT
    query,
    ProfileEvents['SkippingIndexCacheMisses'] > 0 AS has_misses,
    ProfileEvents['SkippingIndexCacheMisses'] = 0 AND ProfileEvents['SkippingIndexCacheHits'] > 0 AS only_hits
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600 AND current_database = currentDatabase() AND type = 'QueryFinish'
    AND query LIKE 'SELECT count() FROM tab WHERE%' AND query NOT LIKE '%use_skip_indexes = 0%'
ORDER BY event_time_microseconds;

DROP TABLE tab;
