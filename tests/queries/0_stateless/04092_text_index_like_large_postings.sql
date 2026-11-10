-- Tags: no-parallel-replicas

-- Test that LIKE pattern queries work correctly with text index when posting lists
-- span multiple blocks (large postings). Previously, pattern-matched tokens with
-- multi-block posting lists were not getting their reading streams initialized,
-- resulting in incorrectly empty posting lists and false filtering of all rows.

SET enable_analyzer = 1;

-- Force the direct read from the text index. CI may inject these as False, in which case
-- the query would just scan 's' and the comparison below would pass without exercising the
-- index reader at all. The multi-block reading bug under test only manifests on the direct
-- read path ('MergeTreeReaderTextIndex'), so it must be enabled here.
SET use_skip_indexes = 1;
SET use_skip_indexes_on_data_read = 1;
SET query_plan_direct_read_from_text_index = 1;
SET use_text_index_like_evaluation_by_dictionary_scan = 1;

DROP TABLE IF EXISTS t_text_index_like_large;

CREATE TABLE t_text_index_like_large
(
    s String,
    INDEX idx_s s TYPE text(tokenizer = splitByNonAlpha, posting_list_block_size = 1000) GRANULARITY 100000000
)
ENGINE = MergeTree
ORDER BY tuple();

INSERT INTO t_text_index_like_large SELECT 'aaabbbccc' FROM numbers(100000);

-- The matching query must return all rows. Without the fix it returned 0, because the
-- pattern-matched token's multi-block posting list was never read.
SELECT 'matching pattern, with index';
SELECT count() FROM t_text_index_like_large WHERE s LIKE '%abbbc%';
-- Baseline without the index: must return the same count as the indexed query above.
SELECT 'matching pattern, without index';
SELECT count() FROM t_text_index_like_large WHERE s LIKE '%abbbc%' SETTINGS use_skip_indexes = 0;
-- A pattern that should match nothing.
SELECT 'non-matching pattern, with index';
SELECT count() FROM t_text_index_like_large WHERE s LIKE '%xyzcb%';

SYSTEM FLUSH LOGS query_log;

-- Assert that the matching query actually read the (multi-block) posting list from the index.
-- 'TextIndexReadPostings' is incremented in MergeTreeReaderTextIndex::readPostingsBlocksForToken;
-- it stays 0 when the index is bypassed (use_skip_indexes = 0) or when no token matches the pattern.
-- This is what fails if the multi-block reader regresses again.
SELECT query, ProfileEvents['TextIndexReadPostings'] > 0
FROM system.query_log
WHERE current_database = currentDatabase()
    AND type = 'QueryFinish'
    AND event_date >= yesterday()
    AND query LIKE '%SELECT count() FROM t_text_index_like_large%'
ORDER BY event_time_microseconds;

DROP TABLE t_text_index_like_large;
