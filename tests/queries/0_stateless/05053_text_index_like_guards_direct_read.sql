-- Tags: no-parallel-replicas

-- `text_index_like_max_postings_to_read` under the upfront planning flavor: a matched token whose
-- posting blocks the primary key already ruled out is never read, so it must not count.

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
ORDER BY id
-- A tiny randomized granularity made the merge below hit the test timeout in CI.
SETTINGS index_granularity = 8192;

-- 676 tokens 'paa'..'pzz' (~148 rows each, non-embedded postings) plus 'common' in every row;
-- '%pa%' matches 27 of them (3995 rows).
INSERT INTO t_text_index_like_direct
    SELECT number, concat('p', char(97 + (number % 26)), char(97 + intDiv(number, 26) % 26), ' common')
    FROM numbers(100000);
-- The guard events below are counted per part, so the insert must end up as one part.
OPTIMIZE TABLE t_text_index_like_direct FINAL;

-- Budget 0: the analysis-phase scan discards once, and no posting list is read.
SELECT count() FROM t_text_index_like_direct WHERE message LIKE '%pa%'
    SETTINGS log_comment = 'like_direct_q1', text_index_like_max_postings_to_read = 0;

-- A budget the pattern fits in: the whole dictionary is scanned and the 27 matched tokens are counted.
SELECT count() FROM t_text_index_like_direct WHERE message LIKE '%pa%'
    SETTINGS log_comment = 'like_direct_q2', text_index_like_max_postings_to_read = 1000000;

-- The primary key prunes first; `gapfar` spans the surviving ranges, but none of its posting blocks
-- lies in them, so only `gapnear` may count.
DROP TABLE IF EXISTS t_text_index_like_gap;
CREATE TABLE t_text_index_like_gap
(
    id UInt64,
    message String,
    INDEX idx(message) TYPE text(tokenizer = splitByNonAlpha, posting_list_block_size = 500, posting_list_codec = 'none') GRANULARITY 100000000
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1024;

-- `gapfar` holds rows [0, 999] and [150000, 150999], two whole Roaring containers, so at this block
-- size they serialize as two posting blocks with rows 1000..149999 between them and still inside the
-- token's coarse span [0, 150999]. The codec is pinned because a randomized one groups containers
-- differently. `gapnear` holds rows [70000, 70999], one block inside the primary-key window below,
-- which prunes to 2 of the 195 granules.
INSERT INTO t_text_index_like_gap
SELECT number,
       multiIf(number < 1000 OR (number >= 150000 AND number < 151000), 'gapfar',
               number >= 70000 AND number < 71000, 'gapnear',
               'filler')
FROM numbers(200000)
SETTINGS max_insert_threads = 1;
OPTIMIZE TABLE t_text_index_like_gap FINAL;

-- Baseline without the index.
SELECT count() FROM t_text_index_like_gap
WHERE id >= 70000 AND id < 71000 AND message LIKE '%gap%' SETTINGS use_skip_indexes = 0;

-- Q3: only `gapnear` counts, which is exactly the budget, so the scan is not discarded.
SELECT count() FROM t_text_index_like_gap
WHERE id >= 70000 AND id < 71000 AND message LIKE '%gap%'
    SETTINGS log_comment = 'like_direct_q3', text_index_like_min_pattern_length = 3,
             use_text_index_postings_cache = 0, use_text_index_dictionary_cache = 0,
             text_index_like_max_postings_to_read = 1;

-- Q4: the window is on `gapfar`'s second block, so `gapfar` counts and a budget of 0 discards the scan.
SELECT count() FROM t_text_index_like_gap
WHERE id >= 150000 AND id < 151000 AND message LIKE '%gap%'
    SETTINGS log_comment = 'like_direct_q4', text_index_like_min_pattern_length = 3,
             use_text_index_postings_cache = 0, use_text_index_dictionary_cache = 0,
             text_index_like_max_postings_to_read = 0;

SYSTEM FLUSH LOGS query_log;

SELECT 'q1',
    ProfileEvents['TextIndexDiscardPatternScan'] = 1 AS discarded_scan_once,
    ProfileEvents['TextIndexReadPostings'] = 0 AS no_postings_read
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND event_date >= yesterday()
    AND log_comment = 'like_direct_q1';

SELECT 'q2',
    ProfileEvents['TextIndexDiscardPatternScan'] = 0 AS scan_not_discarded,
    ProfileEvents['TextIndexPatternScannedTokens'] AS scanned_tokens,
    ProfileEvents['TextIndexPatternMatchedTokens'] AS matched_tokens
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND event_date >= yesterday()
    AND log_comment = 'like_direct_q2';

SELECT 'q3',
    ProfileEvents['TextIndexDiscardPatternScan'] = 0 AS scan_not_discarded
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND event_date >= yesterday()
    AND log_comment = 'like_direct_q3';

SELECT 'q4',
    ProfileEvents['TextIndexDiscardPatternScan'] = 1 AS discarded_scan_once
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND event_date >= yesterday()
    AND log_comment = 'like_direct_q4';

DROP TABLE t_text_index_like_gap;
DROP TABLE t_text_index_like_direct;
