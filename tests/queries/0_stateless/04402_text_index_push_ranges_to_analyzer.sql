-- Tags: no-parallel-replicas

-- Tests that text index analysis pushes the row ranges still readable after the analysis of the
-- primary key into the TextIndexAnalyzer and fails 'All'-mode queries whose tokens (or their
-- intersection) fall entirely outside those rows. This skips reading their posting lists and
-- short-circuits the dictionary scan, without changing query results.

SET enable_analyzer = 1;
SET use_skip_indexes = 1;
-- The query condition cache remembers which granules failed WHERE from previous runs and would
-- mask the actual index behavior, so disable it for this test.
SET use_query_condition_cache = 0;

DROP TABLE IF EXISTS tab;
DROP TABLE IF EXISTS tab_cost;

CREATE TABLE tab
(
    id UInt64,
    s String,
    INDEX idx_s s TYPE text(tokenizer = splitByNonAlpha)
)
ENGINE = MergeTree ORDER BY id
-- Pin the posting list layout so the rare token is a single block; otherwise a randomized
-- 'text_index_posting_list_block_size' could split it and skip the analyze-time read regardless.
SETTINGS index_granularity = 8192, text_index_posting_list_block_size = 1048576, text_index_posting_list_codec = 'none';

-- A single part where the rare tokens 'needle'/'apple' occur only in the high id range
-- [900000, 1000000), while 'banana' occurs only in the low id range [0, 900000).
INSERT INTO tab SELECT number, if(number >= 900000, 'needle apple', 'filler banana') FROM numbers(1000000);
OPTIMIZE TABLE tab FINAL;

-- Correctness. 'needle'/'apple' are outside 'id < 100000'; 'banana' is inside.
SELECT count() FROM tab WHERE id < 100000 AND hasToken(s, 'needle');                -- All, outside        -> 0
SELECT count() FROM tab WHERE id < 100000 AND hasAllTokens(s, ['needle', 'apple']); -- All, outside        -> 0
SELECT count() FROM tab WHERE hasToken(s, 'needle');                                -- whole part readable -> 100000
SELECT count() FROM tab WHERE id < 100000 AND hasAnyTokens(s, ['needle', 'banana']);-- Any, 'banana' inside -> 100000

-- Equivalence: the same predicates must return the same counts with skip indexes disabled. This
-- proves the optimization never over-prunes, in particular not the 'Any'-mode query.
SELECT count() FROM tab WHERE id < 100000 AND hasToken(s, 'needle') SETTINGS use_skip_indexes = 0;
SELECT count() FROM tab WHERE id < 100000 AND hasAllTokens(s, ['needle', 'apple']) SETTINGS use_skip_indexes = 0;
SELECT count() FROM tab WHERE hasToken(s, 'needle') SETTINGS use_skip_indexes = 0;
SELECT count() FROM tab WHERE id < 100000 AND hasAnyTokens(s, ['needle', 'banana']) SETTINGS use_skip_indexes = 0;

-- The optimization fires: on a fresh table (cold caches), the 'All'-mode query whose token is
-- entirely outside the readable rows skips reading its posting list, while the same query over the
-- whole part reads it. The filtered query runs first so the rare token's postings stay uncached.
CREATE TABLE tab_cost
(
    id UInt64,
    s String,
    INDEX idx_s s TYPE text(tokenizer = splitByNonAlpha)
)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 8192, text_index_posting_list_block_size = 1048576, text_index_posting_list_codec = 'none';

INSERT INTO tab_cost SELECT number, if(number >= 900000, 'needle apple', 'filler banana') FROM numbers(1000000);
OPTIMIZE TABLE tab_cost FINAL;

SELECT count() FROM tab_cost WHERE id < 100000 AND hasToken(s, 'needle');  -- 0,      reads 0 posting blocks
SELECT count() FROM tab_cost WHERE hasToken(s, 'needle');                  -- 100000, reads the posting block

SYSTEM FLUSH LOGS query_log;

-- Posting blocks read by the two 'tab_cost' queries above, in execution order. The filtered query
-- reads none (its token is pruned before the postings are read), the whole-part query reads one.
-- Expected: 0, 1. A leading wildcard is needed because the client stores the preceding comment as
-- part of the query text; the readback excludes its own 'query_log' reference.
SELECT ProfileEvents['TextIndexReadPostings'] > 0
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600
  AND current_database = currentDatabase() AND type = 'QueryFinish'
  AND query LIKE '%hasToken(s, ''needle'')%' AND query LIKE '%FROM tab_cost%' AND query NOT LIKE '%query_log%'
ORDER BY event_time_microseconds;

DROP TABLE tab;
DROP TABLE tab_cost;
