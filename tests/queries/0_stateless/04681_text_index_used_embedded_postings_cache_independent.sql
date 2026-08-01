-- Tags: no-parallel
-- no-parallel: SYSTEM DROP TEXT INDEX TOKENS CACHE is server-global, so a concurrent test could
--              both drop this test's warm entries and warm them between the cold arm's drop and its
--              query.

-- `TextIndexUsedEmbeddedPostings` counts uses of a posting list embedded in the text index
-- dictionary. A use must be counted whether the token info came from the dictionary on disk or from
-- the server-global tokens cache, so the counter must read the same on a cold and on a warm cache.

SET use_skip_indexes_on_data_read = 1;
SET query_plan_direct_read_from_text_index = 1;
SET enable_analyzer = 1;

DROP TABLE IF EXISTS tab;

CREATE TABLE tab
(
    id UInt64,
    str String,
    INDEX idx_str str TYPE text(tokenizer = splitByNonAlpha) GRANULARITY 8
)
ENGINE = MergeTree ORDER BY id;

-- Each token occurs in exactly 5 rows, which is below MAX_CARDINALITY_FOR_EMBEDDED_POSTINGS, so the
-- posting lists are embedded in the dictionary rather than stored separately.
INSERT INTO tab SELECT number, arrayStringConcat(arrayMap(x -> toString(number + x * 2), range(5)), ' ') FROM numbers(100000);

SYSTEM DROP TEXT INDEX TOKENS CACHE;

-- Cold cache: the token info is deserialized from the dictionary and its embedded postings are used.
SELECT count() FROM tab WHERE hasAnyTokens(str, ['34567']) SETTINGS log_comment = '04681_cold';

-- Warm cache: the same token info now comes from the tokens cache, and its embedded postings are
-- used again without any deserialization. The counter must still fire.
SELECT count() FROM tab WHERE hasAnyTokens(str, ['34567']) SETTINGS log_comment = '04681_warm';

SYSTEM FLUSH LOGS query_log;

-- Columns: the counter fired, the cache was hit, the dictionary was read.
-- The cold arm must miss the cache and read the dictionary; the warm arm must do neither, which is
-- what proves its counter increment came from the cache-hit path and not from a repeated read.
-- One row per arm. `argMax` takes the newest row of each arm so a rerun inside one database (CI can
-- pass a fixed --database) reads this run's rows rather than an earlier run's.
SELECT
    if(log_comment = '04681_cold', 'cold', 'warm') AS arm,
    argMax(ProfileEvents['TextIndexUsedEmbeddedPostings'] > 0, event_time_microseconds) AS used_embedded_postings,
    argMax(ProfileEvents['TextIndexTokensCacheHits'] > 0, event_time_microseconds) AS tokens_cache_hits,
    argMax(ProfileEvents['TextIndexTokensCacheMisses'] > 0, event_time_microseconds) AS tokens_cache_misses
FROM system.query_log
WHERE type = 'QueryFinish'
  AND event_date >= yesterday()
  AND current_database = currentDatabase()
  AND log_comment IN ('04681_cold', '04681_warm')
GROUP BY arm
ORDER BY arm;

DROP TABLE tab;
