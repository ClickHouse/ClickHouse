-- Tags: no-parallel-replicas

-- Tests that `analyzePostings` reads single-block posting lists rarest-first, so an `All`-mode query
-- short-circuits right after the rarest token instead of reading the posting list of every token it
-- declares. The number of posting lists read must therefore not grow with the number of tokens in
-- the query. See PR https://github.com/ClickHouse/ClickHouse/pull/112491.

SET enable_analyzer = 1;
SET use_skip_indexes = 1;
SET use_query_condition_cache = 0;
-- Keep the counts below independent of what other queries have already cached.
SET use_text_index_postings_cache = 0;

DROP TABLE IF EXISTS tab_postings_order;

CREATE TABLE tab_postings_order
(
    id UInt64,
    s String,
    INDEX idx_s s TYPE text(tokenizer = splitByNonAlpha)
)
ENGINE = MergeTree ORDER BY id
-- Pin the posting list layout so each token is a single block. `analyzePostings` orders only
-- single-block posting lists; a randomized block size would split the common tokens into several
-- blocks, which are read by another code path and make the counts below independent of the ordering.
SETTINGS index_granularity = 8192, text_index_posting_list_block_size = 1048576;

-- One part with:
--   'common1' .. 'common8' -- all 500 even rows, cardinality 500 each;
--   'zrare'                -- 7 odd rows, cardinality 7;
--   'filler'               -- the remaining 493 odd rows.
-- The row range of 'zrare' ([1, 13]) overlaps the row range of the common tokens ([0, 998]), so the
-- coarse rows_range clip cannot prune the query at the dictionary stage. But the posting lists are
-- disjoint, so the `All`-mode intersection empties as soon as 'zrare' meets any common token.
-- Cardinality 7 is above `MAX_CARDINALITY_FOR_EMBEDDED_POSTINGS`, hence 'zrare' is not embedded into
-- the dictionary and is read by `analyzePostings` like the common tokens.
INSERT INTO tab_postings_order
SELECT
    number,
    if(number % 2 = 0, 'common1 common2 common3 common4 common5 common6 common7 common8', if(number < 14, 'zrare', 'filler'))
FROM numbers(1000);

-- None of the three queries can match. Rarest-first ordering reads 'zrare' first, then one common
-- token empties the intersection and the query is failed, so exactly 2 posting lists are read no
-- matter how many common tokens the query declares. Without the ordering the read order comes from
-- a hash map, the common tokens are folded first and the count grows with the number of tokens
-- (2, 4 and 6 posting lists before the change).
SELECT count() FROM tab_postings_order WHERE hasAllTokens(s, ['common1', 'zrare'])
SETTINGS log_comment = '04695_tokens_2';

SELECT count() FROM tab_postings_order WHERE hasAllTokens(s, ['common1', 'common2', 'common3', 'common4', 'zrare'])
SETTINGS log_comment = '04695_tokens_5';

SELECT count() FROM tab_postings_order WHERE hasAllTokens(s, ['common1', 'common2', 'common3', 'common4', 'common5', 'common6', 'common7', 'common8', 'zrare'])
SETTINGS log_comment = '04695_tokens_9';

SYSTEM FLUSH LOGS query_log;

SELECT log_comment, ProfileEvents['TextIndexReadPostings'] AS postings_read
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600
  AND current_database = currentDatabase() AND type = 'QueryFinish'
  AND log_comment LIKE '04695_tokens_%'
ORDER BY log_comment;

-- Equivalence: reordering the reads does not change the results.
SELECT count() FROM tab_postings_order WHERE hasAllTokens(s, ['common1', 'common2', 'common3', 'common4', 'common5', 'common6', 'common7', 'common8', 'zrare']) SETTINGS use_skip_indexes = 0;
SELECT count() FROM tab_postings_order WHERE hasAllTokens(s, ['common1', 'common8']);
SELECT count() FROM tab_postings_order WHERE hasAllTokens(s, ['filler', 'zrare']);
SELECT count() FROM tab_postings_order WHERE hasAnyTokens(s, ['common1', 'zrare']);

DROP TABLE tab_postings_order;
