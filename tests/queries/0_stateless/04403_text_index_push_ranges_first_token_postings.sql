-- Tags: no-parallel-replicas

-- Tests the precise (posting-level) part of the readable-rows push into the text index analyzer.
-- All searched tokens occur in several ranges spread across the part, so their row-range spans overlap
-- the range selected by the primary key and the coarse rows_range clip cannot prune them. But the tokens
-- are absent from the exact range the primary key selects, so the 'All'-mode query fails after reading
-- the first token's postings (its intersection with the readable rows is empty) and the remaining tokens'
-- posting lists are not read. The test asserts exactly one posting block is read.

SET enable_analyzer = 1;
SET use_skip_indexes = 1;
SET use_query_condition_cache = 0;

DROP TABLE IF EXISTS tab_multi;

CREATE TABLE tab_multi
(
    id UInt64,
    s String,
    INDEX idx_s s TYPE text(tokenizer = splitByNonAlpha)
)
ENGINE = MergeTree ORDER BY id
-- Pin the posting list layout so each token is a single block (otherwise a randomized block size could
-- split it and skip the analyze-time read regardless of the optimization).
SETTINGS index_granularity = 8192, text_index_posting_list_block_size = 1048576, text_index_posting_list_codec = 'none';

-- 'alpha'/'beta' occur in [0, 300000) and [700000, 1000000), but are absent from the middle [300000, 700000).
-- Their row-range spans are therefore [0, ~1000000), overlapping any selected range in the middle.
INSERT INTO tab_multi SELECT number, if(number < 300000 OR number >= 700000, 'alpha beta gamma', 'filler text') FROM numbers(1000000);
OPTIMIZE TABLE tab_multi FINAL;

-- The primary key selects [400000, 500000), which lies inside the gap where the tokens are absent.
-- rows_range cannot prune (spans overlap); the query fails on the first token's empty readable intersection.
SELECT count() FROM tab_multi WHERE id >= 400000 AND id < 500000 AND hasAllTokens(s, 'alpha beta gamma');

SYSTEM FLUSH LOGS query_log;

-- Posting blocks read by the query above. Exactly 1: the second token is skipped after the first fails.
-- (A value of 0 would mean the coarse rows_range clip pruned it instead; > 1 would mean no early exit.)
SELECT ProfileEvents['TextIndexReadPostings']
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600
  AND current_database = currentDatabase() AND type = 'QueryFinish'
  AND query LIKE '%hasAllTokens(s, ''alpha beta gamma'')%' AND query LIKE '%FROM tab_multi%'
  AND query NOT LIKE '%use_skip_indexes%' AND query NOT LIKE '%query_log%'
ORDER BY event_time_microseconds;

-- Equivalence: the same predicate returns the same count with skip indexes disabled.
SELECT count() FROM tab_multi WHERE id >= 400000 AND id < 500000 AND hasAllTokens(s, 'alpha beta gamma') SETTINGS use_skip_indexes = 0;

-- 'Any'-mode counterpart. The same tokens overlap the selected range coarsely but have no readable
-- occurrence, so once none of them can contribute the 'Any' query is failed and the text index prunes
-- every selected mark — rather than keeping them on the stale clipped row range. 'Any' does not
-- short-circuit the dictionary scan (every token is read), so the observable signal is that the main
-- query reads no rows: read_rows must be 0.
SELECT count() FROM tab_multi WHERE id >= 400000 AND id < 500000 AND hasAnyTokens(s, 'alpha beta gamma');

SYSTEM FLUSH LOGS query_log;

SELECT read_rows
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600
  AND current_database = currentDatabase() AND type = 'QueryFinish'
  AND query LIKE '%hasAnyTokens(s, ''alpha beta gamma'')%' AND query LIKE '%FROM tab_multi%'
  AND query NOT LIKE '%use_skip_indexes%' AND query NOT LIKE '%query_log%'
ORDER BY event_time_microseconds;

SELECT count() FROM tab_multi WHERE id >= 400000 AND id < 500000 AND hasAnyTokens(s, 'alpha beta gamma') SETTINGS use_skip_indexes = 0;

-- Duplicate-token 'Any'. A repeated token must be counted once towards "no contributing tokens remain",
-- otherwise the query is never failed and the marks are kept. Same gap scenario, so read_rows must be 0.
SELECT count() FROM tab_multi WHERE id >= 400000 AND id < 500000 AND hasAnyTokens(s, ['alpha', 'alpha']);

SYSTEM FLUSH LOGS query_log;

SELECT read_rows
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600
  AND current_database = currentDatabase() AND type = 'QueryFinish'
  AND query LIKE '%hasAnyTokens(s, [''alpha'', ''alpha''])%' AND query LIKE '%FROM tab_multi%'
  AND query NOT LIKE '%use_skip_indexes%' AND query NOT LIKE '%query_log%'
ORDER BY event_time_microseconds;

SELECT count() FROM tab_multi WHERE id >= 400000 AND id < 500000 AND hasAnyTokens(s, ['alpha', 'alpha']) SETTINGS use_skip_indexes = 0;

DROP TABLE tab_multi;
