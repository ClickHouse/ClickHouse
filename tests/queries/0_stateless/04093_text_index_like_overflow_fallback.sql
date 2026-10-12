-- Tags: no-parallel-replicas

-- Test that LIKE pattern queries correctly fall back to brute force evaluation
-- when the number of matched tokens with large postings exceeds
-- text_index_like_max_postings_to_read. The fallback must produce the same
-- results as a full scan without the index.

SET use_text_index_like_evaluation_by_dictionary_scan = 1;

-- The patterns below ('%pa%', '%p%') are shorter than the default minimum length, which
-- would otherwise reject them before the dictionary scan; lower the threshold so the scan runs.
SET text_index_like_min_pattern_length = 1;

-- Force the direct read from the text index. CI may inject these as False, in which case the
-- query would just scan 'message' and the dictionary-scan overflow / fallback under test would
-- never be reached, so the comparison below would pass without exercising the index.
SET use_skip_indexes = 1;
SET use_skip_indexes_on_data_read = 1;
SET query_plan_direct_read_from_text_index = 1;

DROP TABLE IF EXISTS t_text_index_like_overflow;

CREATE TABLE t_text_index_like_overflow
(
    id UInt64,
    message String,
    INDEX idx(message) TYPE text(tokenizer = splitByNonAlpha)
)
ENGINE = MergeTree
ORDER BY id;

-- Generate 100 000 rows with 676 unique purely-alphabetic tokens ('paa' .. 'pzz').
-- Each token appears ~148 times across ~13 granules (index_granularity = 8192),
-- so most tokens will have non-embedded (large) postings.
INSERT INTO t_text_index_like_overflow
    SELECT number, concat('p', char(97 + (number % 26)), char(97 + intDiv(number, 26) % 26))
    FROM numbers(100000);

-- With max_postings_to_read = 1 the dictionary scan overflows almost immediately
-- because '%pa%' matches several tokens and most have large postings.
-- The fallback to brute force must return the same count as a plain scan.
SELECT 'LIKE with overflow';
SELECT count() FROM t_text_index_like_overflow WHERE message LIKE '%pa%' SETTINGS text_index_like_max_postings_to_read = 1;
SELECT count() FROM t_text_index_like_overflow WHERE message LIKE '%pa%' SETTINGS use_skip_indexes = 0;

-- NOT LIKE cannot be answered from the text index (a negated substring match has no usable
-- bound on the rows), so it never enters the dictionary-scan pattern path: no overflow, no
-- discard (see the TextIndexDiscardPatternScan assertion below, where both NOT LIKE rows are 0).
-- It always falls back to a full scan, but must still return the correct count.
SELECT 'NOT LIKE (always full scan)';
SELECT count() FROM t_text_index_like_overflow WHERE message NOT LIKE '%pa%' SETTINGS text_index_like_max_postings_to_read = 1;
SELECT count() FROM t_text_index_like_overflow WHERE message NOT LIKE '%pa%' SETTINGS use_skip_indexes = 0;

-- A broader pattern that matches ALL tokens overflows even faster.
SELECT 'broad LIKE with overflow';
SELECT count() FROM t_text_index_like_overflow WHERE message LIKE '%p%' SETTINGS text_index_like_max_postings_to_read = 1;
SELECT count() FROM t_text_index_like_overflow WHERE message LIKE '%p%' SETTINGS use_skip_indexes = 0;

-- With a generous threshold the index can serve the query without fallback;
-- results must still be correct.
SELECT 'LIKE without overflow';
SELECT count() FROM t_text_index_like_overflow WHERE message LIKE '%pa%' SETTINGS text_index_like_max_postings_to_read = 10000;
SELECT count() FROM t_text_index_like_overflow WHERE message LIKE '%pa%' SETTINGS use_skip_indexes = 0;

SYSTEM FLUSH LOGS query_log;

-- Assert which queries actually overflowed the dictionary scan and fell back to brute force.
-- 'TextIndexDiscardPatternScan' is incremented when the number of large postings to read
-- exceeds 'text_index_like_max_postings_to_read'. It is set for the overflowing LIKE queries
-- (max_postings_to_read = 1) and unset everywhere else: the generous-threshold query (no
-- overflow), the use_skip_indexes = 0 baselines (index not touched), and the NOT LIKE queries
-- (which never enter the dictionary-scan pattern path).
SELECT query, ProfileEvents['TextIndexDiscardPatternScan'] > 0
FROM system.query_log
WHERE current_database = currentDatabase()
    AND type = 'QueryFinish'
    AND event_date >= yesterday()
    AND query LIKE '%SELECT count() FROM t_text_index_like_overflow%'
ORDER BY event_time_microseconds;

DROP TABLE t_text_index_like_overflow;
