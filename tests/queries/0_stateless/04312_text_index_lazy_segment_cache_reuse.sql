-- Tags: no-parallel, no-parallel-replicas
-- no-parallel: uses `SYSTEM CLEAR TEXT INDEX POSTINGS CACHE`, which resets the server-wide
--   cache for every concurrently running query.
-- no-parallel-replicas: parallel replicas split the granule across replicas, so the per-segment
--   build/cache counts asserted below would no longer be deterministic.

-- Regression guard for the compressed-segment branch of `PostingListCursor::prepareSegment`:
-- decoded `PostingListSegment`s are shared through `TextIndexPostingsCache`, keyed by index id +
-- segment file offset. Reading the same compressed segment a second time with a shared cache must
-- hit the cache and skip `buildPostingSegment` entirely, so the second execution decodes zero new
-- segments (`TextIndexLazySegmentsBuilt = 0`) while `TextIndexPostingsCacheHits > 0`.
--
-- Asserting only that `TextIndexLazySegmentsBuilt > 0` (as 04257_text_index_lazy_profile_events
-- does) would still pass if every cursor missed the cache and rebuilt the segment each time, so it
-- would not pin the cache key and variant wiring that `prepareSegment` adds.

SET enable_full_text_index = 1;
SET allow_experimental_text_index_lazy_apply = 1;
SET text_index_posting_list_apply_mode = 'lazy';
SET query_plan_direct_read_from_text_index = 1;
SET use_text_index_postings_cache = 1;
SET use_query_condition_cache = 0;
SET merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 0;
SET log_queries = 1;

DROP TABLE IF EXISTS tab_seg_cache;

-- posting_list_block_size = 256 -> tokens with cardinality > 256 are stored compressed across
-- multiple segments and read through the lazy cursor (the only path that consults the postings
-- cache). index_granularity keeps all rows in a single granule.
CREATE TABLE tab_seg_cache(
    k UInt64,
    s String,
    INDEX idx s TYPE text(
        tokenizer = 'splitByNonAlpha',
        posting_list_codec = 'bitpacking',
        posting_list_block_size = 256))
ENGINE = MergeTree() ORDER BY k
SETTINGS index_granularity = 8192, index_granularity_bytes = '10M';

-- 'lazytok' lands on every 4th row -> cardinality 512 (> 256, so compressed/multi-segment) and
-- density ~0.25 (not dense, so the cursor really decodes blocks instead of taking the dense shortcut).
INSERT INTO tab_seg_cache
SELECT number, if(number % 4 = 0, 'lazytok', 'other')
FROM numbers(2048);

SELECT 'cardinality lazytok', count() FROM tab_seg_cache WHERE hasToken(s, 'lazytok');

-- Start from an empty cache so the first execution is a guaranteed cold miss.
SYSTEM CLEAR TEXT INDEX POSTINGS CACHE;

-- Cold: every segment is a cache miss and gets built once.
SELECT count() FROM tab_seg_cache WHERE hasToken(s, 'lazytok')
    SETTINGS log_comment = '04312_seg_cache_cold';

-- Warm: the same segments are now shared through the cache, so nothing is rebuilt.
SELECT count() FROM tab_seg_cache WHERE hasToken(s, 'lazytok')
    SETTINGS log_comment = '04312_seg_cache_warm';

SYSTEM FLUSH LOGS query_log;

SELECT 'cold: built segments, missed cache, no hits';
SELECT
    sum(ProfileEvents['TextIndexLazySegmentsBuilt'])    > 0 AS segments_built,
    sum(ProfileEvents['TextIndexPostingsCacheMisses'])  > 0 AS cache_missed,
    sum(ProfileEvents['TextIndexPostingsCacheHits'])    = 0 AS no_cache_hits
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600
  AND current_database = currentDatabase()
  AND type = 'QueryFinish'
  AND log_comment = '04312_seg_cache_cold';

SELECT 'warm: reused cached segments, built nothing';
SELECT
    sum(ProfileEvents['TextIndexLazySegmentsBuilt'])    = 0 AS no_segments_built,
    sum(ProfileEvents['TextIndexPostingsCacheHits'])    > 0 AS cache_hit,
    sum(ProfileEvents['TextIndexPostingsCacheMisses'])  = 0 AS no_cache_misses
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600
  AND current_database = currentDatabase()
  AND type = 'QueryFinish'
  AND log_comment = '04312_seg_cache_warm';

SYSTEM CLEAR TEXT INDEX POSTINGS CACHE;
DROP TABLE tab_seg_cache;
