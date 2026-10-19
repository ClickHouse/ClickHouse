-- Tags: no-parallel-replicas

-- Regression test for the readable-rows push: an 'Any'-mode query whose only token has no readable
-- occurrence (its rows fall entirely outside the range selected by the primary key) must drop the
-- query-token edge so the token's posting list is NOT read, and the query is failed. Without this,
-- analyzePostings would still read the single-block postings the optimization is meant to avoid.

SET enable_analyzer = 1;
SET use_skip_indexes = 1;
SET use_query_condition_cache = 0;

DROP TABLE IF EXISTS tab;

CREATE TABLE tab
(
    id UInt64,
    s String,
    INDEX idx_s s TYPE text(tokenizer = splitByNonAlpha)
)
ENGINE = MergeTree ORDER BY id
-- Pin the posting list layout so the token is a single block (otherwise a randomized block size could
-- split it and skip the analyze-time read regardless of the optimization).
SETTINGS index_granularity = 8192, text_index_posting_list_block_size = 1048576, text_index_posting_list_codec = 'none';

-- 'needle' occurs only in id >= 900000.
INSERT INTO tab SELECT number, if(number >= 900000, 'needle apple', 'filler banana') FROM numbers(1000000);
OPTIMIZE TABLE tab FINAL;

-- 'Any' query, single token outside the readable range -> no readable occurrence -> result 0 and the
-- posting list is not read. Run before the whole-part query so 'needle' postings stay uncached.
SELECT count() FROM tab WHERE id < 100000 AND hasAnyTokens(s, 'needle');
-- Whole part is readable -> the optimization is a no-op and the token's posting list is read.
SELECT count() FROM tab WHERE hasAnyTokens(s, 'needle');

SYSTEM FLUSH LOGS query_log;

-- Posting blocks read by the two queries above, in execution order. Expected: 0 (skipped), 1 (read).
SELECT ProfileEvents['TextIndexReadPostings']
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600
  AND current_database = currentDatabase() AND type = 'QueryFinish'
  AND query LIKE '%hasAnyTokens(s, ''needle'')%' AND query LIKE '%FROM tab%'
  AND query NOT LIKE '%use_skip_indexes%' AND query NOT LIKE '%query_log%'
ORDER BY event_time_microseconds;

-- Equivalence: the same predicates return the same counts with skip indexes disabled.
SELECT count() FROM tab WHERE id < 100000 AND hasAnyTokens(s, 'needle') SETTINGS use_skip_indexes = 0;
SELECT count() FROM tab WHERE hasAnyTokens(s, 'needle') SETTINGS use_skip_indexes = 0;

DROP TABLE tab;
