-- Tags: no-parallel-replicas
-- no-parallel-replicas: parallel replicas split the part between replicas, each with its own query-local
--   cache, so the segments at the replica boundaries are built more than once and the count below is not
--   deterministic.

-- The postings cache that a query creates for itself when `use_text_index_postings_cache = 0` is sized as 10%
-- of `max_memory_usage`. The cache used to be SLRU with the protected queue as large as the whole cache: a
-- segment that was prepared twice was promoted to the protected queue and stayed there for good, and once the
-- promoted entries filled the cache every later insert was evicted right after it landed in the probationary
-- queue, so every later segment was read from disk again on its next preparation. With plain LRU a segment is
-- built exactly once even when the segments of the query do not fit into the cache.
--
-- The query below prepares almost every segment twice regardless of how the read pool splits the part: the two
-- `hasAllTokens` are two independent search queries, the reader keeps a separate posting list cursor per token
-- per search query, and the 31 tokens the two share are walked twice over the same rows. `segments_reused`
-- asserts that the second walk really goes through the cache instead of the disk.

SET enable_full_text_index = 1;
SET log_queries = 1;
-- A single part: the segment count below assumes that every token has exactly 16384 postings in one part.
SET max_insert_threads = 1;

DROP TABLE IF EXISTS tab_local_postings_cache;

-- Every row has all 32 tokens, so each token has 16384 / 128 = 128 segments of 128 rows. The 4096 segments
-- weigh about 4.8 MB in the cache, while `max_memory_usage = 30 MB` gives the query-local cache 3 MB: enough
-- for the segments of the granule being read, but not for all of them, so a policy that pins entries runs out
-- of space halfway through the part.
CREATE TABLE tab_local_postings_cache
(
    k UInt64,
    s String,
    INDEX idx s TYPE text(tokenizer = 'splitByNonAlpha', posting_list_codec = 'bitpacking', posting_list_block_size = 128)
)
ENGINE = MergeTree ORDER BY k
SETTINGS index_granularity = 32, index_granularity_bytes = '10M', min_bytes_for_wide_part = 0;

INSERT INTO tab_local_postings_cache
SELECT number, arrayStringConcat(arrayMap(i -> 'tok' || toString(i), range(32)), ' ')
FROM numbers(16384);

SELECT 'parts', count() FROM system.parts WHERE database = currentDatabase() AND table = 'tab_local_postings_cache' AND active;

SELECT count() FROM tab_local_postings_cache
WHERE hasAllTokens(s, ['tok0', 'tok1', 'tok2', 'tok3', 'tok4', 'tok5', 'tok6', 'tok7', 'tok8', 'tok9', 'tok10', 'tok11', 'tok12', 'tok13', 'tok14', 'tok15',
                       'tok16', 'tok17', 'tok18', 'tok19', 'tok20', 'tok21', 'tok22', 'tok23', 'tok24', 'tok25', 'tok26', 'tok27', 'tok28', 'tok29', 'tok30', 'tok31'])
  AND hasAllTokens(s, ['tok1', 'tok2', 'tok3', 'tok4', 'tok5', 'tok6', 'tok7', 'tok8', 'tok9', 'tok10', 'tok11', 'tok12', 'tok13', 'tok14', 'tok15',
                       'tok16', 'tok17', 'tok18', 'tok19', 'tok20', 'tok21', 'tok22', 'tok23', 'tok24', 'tok25', 'tok26', 'tok27', 'tok28', 'tok29', 'tok30', 'tok31'])
SETTINGS
    text_index_posting_list_apply_mode = 'lazy',
    query_plan_direct_read_from_text_index = 1,
    use_skip_indexes_on_data_read = 1,
    use_query_condition_cache = 0,
    use_text_index_postings_cache = 0,
    merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 0,
    max_threads = 1,
    max_memory_usage = 30000000,
    log_comment = '05229_local_postings_cache';

SYSTEM FLUSH LOGS query_log;

-- Every segment was read and decoded exactly once (32 tokens * 128 segments), and the repeated preparations
-- of the 31 shared tokens were served from the query-local cache.
SELECT
    ProfileEvents['TextIndexLazySegmentsBuilt'] AS segments_built,
    ProfileEvents['TextIndexLazySegmentsPrepared'] > ProfileEvents['TextIndexLazySegmentsBuilt'] AS segments_reused
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600
  AND current_database = currentDatabase()
  AND type = 'QueryFinish'
  AND log_comment = '05229_local_postings_cache';

DROP TABLE tab_local_postings_cache;
