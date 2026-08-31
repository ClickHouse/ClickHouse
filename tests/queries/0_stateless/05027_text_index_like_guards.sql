-- Tags: no-parallel-replicas

-- Test the LIKE dictionary-scan guards and the selectivity short-circuit:
-- 1. `text_index_like_max_postings_rows_to_read` bounds the total posting rows read (charged by
--    cardinality), cutting the scan short and falling back to brute force.
-- 2. `analyzeCardinalitiesAndBypassPatterns` bypasses a pattern query whose matched-token union
--    leaves no row unmatched, before any posting list is read.
-- In every case the result must equal a plain scan without the index.

SET use_text_index_like_evaluation_by_dictionary_scan = 1;
SET use_skip_indexes = 1;
SET use_skip_indexes_on_data_read = 1;
SET query_plan_direct_read_from_text_index = 1;
-- Allow the short needles below into the dictionary scan (default minimum length is 4).
SET text_index_like_min_pattern_length = 2;

DROP TABLE IF EXISTS t_text_index_like_guards;

CREATE TABLE t_text_index_like_guards
(
    id UInt64,
    message String,
    INDEX idx(message) TYPE text(tokenizer = splitByNonAlpha)
)
ENGINE = MergeTree
ORDER BY id;

-- 100 000 rows, 676 unique alphabetic tokens ('paa'..'pzz'), each ~148 rows (~13 granules),
-- so most tokens have non-embedded postings; '%pa%' matches 27 of them (3995 rows).
-- Every row also carries the token 'common', whose posting list covers the whole part.
INSERT INTO t_text_index_like_guards
    SELECT number, concat('p', char(97 + (number % 26)), char(97 + intDiv(number, 26) % 26), ' common')
    FROM numbers(100000);

-- Rows budget = 0: any non-embedded posting row overflows immediately -> discard + fallback.
SELECT count() FROM t_text_index_like_guards WHERE message LIKE '%pa%'
    SETTINGS log_comment = 'like_guards_q1', text_index_like_max_postings_to_read = 1000000, text_index_like_max_postings_rows_to_read = 0;

-- 'common' is in every row, so its postings cannot prune -> bypassed before postings reads.
SELECT count() FROM t_text_index_like_guards WHERE message LIKE '%common%'
    SETTINGS log_comment = 'like_guards_q2', text_index_like_max_postings_to_read = 1000000;

-- Selective token needle: the index serves it, no bypass, correct result.
SELECT count() FROM t_text_index_like_guards WHERE message LIKE '%paa%' SETTINGS text_index_like_min_pattern_length = 3;

SYSTEM FLUSH LOGS query_log;

-- Q1 (rows budget): TextIndexDiscardPatternScan set; Q2 (covers every row): TextIndexDiscardPatternQueryLowSelectivity set.
-- `no_postings_read` is the contract itself: both guards must act *before* any posting list is
-- read, so a regression that materializes postings and only then discards would still set the
-- counters above while failing here.
SELECT 'q1',
    ProfileEvents['TextIndexDiscardPatternScan'] > 0 AS discarded_scan,
    ProfileEvents['TextIndexDiscardPatternQueryLowSelectivity'] > 0 AS bypassed_low_selectivity,
    ProfileEvents['TextIndexReadPostings'] = 0 AS no_postings_read
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND event_date >= yesterday()
    AND log_comment = 'like_guards_q1';

SELECT 'q2',
    ProfileEvents['TextIndexDiscardPatternScan'] > 0 AS discarded_scan,
    ProfileEvents['TextIndexDiscardPatternQueryLowSelectivity'] > 0 AS bypassed_low_selectivity,
    ProfileEvents['TextIndexReadPostings'] = 0 AS no_postings_read
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND event_date >= yesterday()
    AND log_comment = 'like_guards_q2';

DROP TABLE t_text_index_like_guards;
