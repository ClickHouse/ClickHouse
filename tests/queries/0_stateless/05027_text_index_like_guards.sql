-- Tags: no-parallel-replicas

-- Test the LIKE dictionary-scan guards and the selectivity short-circuit:
-- 1. `text_index_like_max_postings_rows_to_read` bounds the total posting rows read (charged by
--    cardinality), cutting the scan short and falling back to brute force.
-- 2. `analyzeCardinalitiesAndBypassPatterns` bypasses a pattern query whose matched-token union is
--    not selective, before any posting list is read.
-- 3. The scan exposes its work via TextIndexPatternScannedTokens / TextIndexPatternMatchedTokens.
-- In every case the result must equal a plain scan without the index.

SET use_text_index_like_evaluation_by_dictionary_scan = 1;
SET use_skip_indexes = 1;
SET use_skip_indexes_on_data_read = 1;
SET query_plan_direct_read_from_text_index = 1;

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
-- so most tokens have non-embedded postings; '%pa%' matches many of them (non-selective).
INSERT INTO t_text_index_like_guards
    SELECT number, concat('p', char(97 + (number % 26)), char(97 + intDiv(number, 26) % 26))
    FROM numbers(100000);

-- Rows budget = 0: any non-embedded posting row overflows immediately -> discard + fallback.
SELECT count() FROM t_text_index_like_guards WHERE message LIKE '%pa%'
    SETTINGS text_index_like_max_postings_to_read = 1000000, text_index_like_max_postings_rows_to_read = 0;

-- Non-selective pattern ('%p%' matches all 100 000 rows) -> bypassed before postings reads.
SELECT count() FROM t_text_index_like_guards WHERE message LIKE '%p%'
    SETTINGS text_index_like_min_pattern_length = 1, text_index_like_max_postings_to_read = 1000000;

-- Selective token needle: the index serves it, no bypass, correct result.
SELECT count() FROM t_text_index_like_guards WHERE message LIKE '%paa%';

SYSTEM FLUSH LOGS query_log;

-- Rows-budget overflow: TextIndexDiscardPatternScan set; selective bypass: TextIndexDiscardPatternQueryLowSelectivity set.
SELECT
    ProfileEvents['TextIndexDiscardPatternScan'] > 0 AS discarded_scan,
    ProfileEvents['TextIndexDiscardPatternQueryLowSelectivity'] > 0 AS bypassed_low_selectivity,
    ProfileEvents['TextIndexPatternScannedTokens'] > 0 AS scanned_tokens
FROM system.query_log
WHERE current_database = currentDatabase()
    AND type = 'QueryFinish'
    AND event_date >= yesterday()
    AND query LIKE 'SELECT count() FROM t_text_index_like_guards WHERE message LIKE \'%%pa%%\'%';

DROP TABLE t_text_index_like_guards;
