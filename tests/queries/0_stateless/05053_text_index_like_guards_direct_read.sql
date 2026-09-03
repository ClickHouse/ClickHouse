-- Tags: no-parallel-replicas

-- The LIKE selectivity guards must run exactly once, in the index-analysis phase, whichever
-- planning flavor is active. 05027 covers `use_skip_indexes_on_data_read = 1` (granules reach the
-- direct reader through the index-read-result pool); this test pins the upfront flavor, where they
-- travel through `read_hints` instead. In both, the reader adopts the analysis-phase granule, and
-- its own deserialize - where the guards stay disabled for lack of pruning context - is never
-- entered by an ordinary query, so the guard events below must count exactly one firing.

SET use_text_index_like_evaluation_by_dictionary_scan = 1;
SET use_skip_indexes = 1;
SET use_skip_indexes_on_data_read = 0;
SET query_plan_direct_read_from_text_index = 1;
-- Allow the short needles below into the dictionary scan (default minimum length is 4).
SET text_index_like_min_pattern_length = 2;

DROP TABLE IF EXISTS t_text_index_like_direct;

CREATE TABLE t_text_index_like_direct
(
    id UInt64,
    message String,
    INDEX idx(message) TYPE text(tokenizer = splitByNonAlpha)
)
ENGINE = MergeTree
ORDER BY id;

-- Same data shape as 05027: 676 tokens 'paa'..'pzz' (~148 rows each, non-embedded postings),
-- '%pa%' matches 27 of them (3995 rows), and 'common' is a token of every row.
INSERT INTO t_text_index_like_direct
    SELECT number, concat('p', char(97 + (number % 26)), char(97 + intDiv(number, 26) % 26), ' common')
    FROM numbers(100000);

-- Rows budget 0: the analysis-phase scan discards once; the adopted granule must not be
-- re-guarded on the read side, and no posting list may be read.
SELECT count() FROM t_text_index_like_direct WHERE message LIKE '%pa%'
    SETTINGS log_comment = 'like_direct_q1', text_index_like_max_postings_to_read = 1000000, text_index_like_max_postings_rows_to_read = 0;

-- 'common' covers the part: the exact-proof bypass fires once, before any posting read.
SELECT count() FROM t_text_index_like_direct WHERE message LIKE '%common%'
    SETTINGS log_comment = 'like_direct_q2', text_index_like_max_postings_to_read = 1000000;

SYSTEM FLUSH LOGS query_log;

SELECT 'q1',
    ProfileEvents['TextIndexDiscardPatternScan'] = 1 AS discarded_scan_once,
    ProfileEvents['TextIndexDiscardPatternQueryLowSelectivity'] = 0 AS no_bypass,
    ProfileEvents['TextIndexReadPostings'] = 0 AS no_postings_read
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND event_date >= yesterday()
    AND log_comment = 'like_direct_q1';

SELECT 'q2',
    ProfileEvents['TextIndexDiscardPatternScan'] = 0 AS no_discarded_scan,
    ProfileEvents['TextIndexDiscardPatternQueryLowSelectivity'] = 1 AS bypassed_once,
    ProfileEvents['TextIndexReadPostings'] = 0 AS no_postings_read
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND event_date >= yesterday()
    AND log_comment = 'like_direct_q2';

DROP TABLE t_text_index_like_direct;
