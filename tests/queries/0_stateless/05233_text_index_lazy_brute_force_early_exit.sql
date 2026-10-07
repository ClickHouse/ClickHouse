-- The brute-force lazy intersection (`intersectBruteForce`) stops as soon as one posting list has no rows
-- in the current window: the intersection over the window is then empty, so the remaining posting lists
-- are not scanned and the partially filled counters are zeroed (or left untouched when already the first
-- posting list is empty). Every result is compared with a plain column scan, and the early exits are
-- observed through the `TextIndexLazyBruteForceEarlyExits` profile event.
--
-- Marks outside the row range of a token's posting-list segments never reach the cursors, so the early exit
-- matters for marks inside a gap of a token: the segment straddling the gap covers those marks by range but
-- has no rows in them.

SET enable_full_text_index = 1;
SET use_skip_indexes = 1;
SET use_skip_indexes_on_data_read = 1;
SET query_plan_direct_read_from_text_index = 1;
SET query_plan_optimize_count_from_text_index = 0;
SET use_query_condition_cache = 0;
SET merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 0;

DROP TABLE IF EXISTS tab_bf_early_exit;

-- One index granule covers the whole part, so the skip index prunes no mark and every mark of the part
-- reaches the intersection with all posting lists.
CREATE TABLE tab_bf_early_exit
(
    id UInt64,
    s String,
    INDEX idx s TYPE text(tokenizer = splitByNonAlpha, posting_list_codec = 'bitpacking', posting_list_block_size = 1024) GRANULARITY 100000000
)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';

-- The tokens come from a hash map, so the order of the cursors in the brute-force intersection is not
-- defined: `bgap` is either the first cursor (the output stays zero-filled) or a later one (the counters
-- are zeroed).
--   aall : every row                                  -> present in every mark
--   bgap : rows [0, 50000) and [150000, 200000)       -> the marks inside the gap are covered by the range
--                                                        of one segment, but contain none of its rows
--   codd : every odd row                              -> present in every mark
INSERT INTO tab_bf_early_exit
SELECT number,
    concat('aall',
        if(number < 50000 OR number >= 150000, ' bgap', ''),
        if(number % 2 = 1, ' codd', ''))
FROM numbers(200000)
SETTINGS max_insert_threads = 1, max_insert_block_size = 1000000, min_insert_block_size_rows = 1000000, min_insert_block_size_bytes = 0;

SELECT 'parts', count() FROM system.parts WHERE database = currentDatabase() AND table = 'tab_bf_early_exit' AND active;

SELECT 'Ground truth (no index)';
SET use_skip_indexes = 0;
SELECT 'all bgap codd', count(), sum(id) FROM tab_bf_early_exit WHERE hasAllTokens(s, ['bgap', 'codd']);
SELECT 'all aall bgap', count(), sum(id) FROM tab_bf_early_exit WHERE hasAllTokens(s, ['aall', 'bgap']);
SELECT 'all aall bgap codd', count(), sum(id) FROM tab_bf_early_exit WHERE hasAllTokens(s, ['aall', 'bgap', 'codd']);
SELECT 'all aall codd', count(), sum(id) FROM tab_bf_early_exit WHERE hasAllTokens(s, ['aall', 'codd']);

SELECT 'Lazy, brute-force intersection';
SET use_skip_indexes = 1;
SET text_index_posting_list_apply_mode = 'lazy';
SET text_index_postings_intersection_algorithm = 'bruteforce';

-- `bgap` is empty in the 11 marks inside the gap: the intersection stops there in every one of them.
SELECT 'all bgap codd', count(), sum(id) FROM tab_bf_early_exit WHERE hasAllTokens(s, ['bgap', 'codd'])
    SETTINGS log_comment = '05233_bf_bgap_codd';

SELECT 'all aall bgap', count(), sum(id) FROM tab_bf_early_exit WHERE hasAllTokens(s, ['aall', 'bgap'])
    SETTINGS log_comment = '05233_bf_aall_bgap';

SELECT 'all aall bgap codd', count(), sum(id) FROM tab_bf_early_exit WHERE hasAllTokens(s, ['aall', 'bgap', 'codd'])
    SETTINGS log_comment = '05233_bf_aall_bgap_codd';

-- Both posting lists have rows in every mark: no early exit.
SELECT 'all aall codd', count(), sum(id) FROM tab_bf_early_exit WHERE hasAllTokens(s, ['aall', 'codd'])
    SETTINGS log_comment = '05233_bf_aall_codd';

SYSTEM FLUSH LOGS query_log;

-- The counters are incremented on whichever replica reads the marks, so under parallel replicas they land
-- on secondary rows. Resolve the initiator rows by `current_database`, then aggregate every row of those
-- queries via `initial_query_id`.
WITH initial_queries AS
(
    SELECT query_id, log_comment
    FROM system.query_log
    WHERE event_date >= yesterday() AND event_time >= now() - 600
      AND current_database = currentDatabase()
      AND type = 'QueryFinish'
      AND is_initial_query = 1
      AND log_comment LIKE '05233_bf_%'
)
SELECT
    q.log_comment,
    sum(ProfileEvents['TextIndexLazyBruteForceIntersections']) > 0 AS brute_force,
    sum(ProfileEvents['TextIndexLazyBruteForceEarlyExits']) > 0 AS early_exits
FROM system.query_log AS l
INNER JOIN initial_queries AS q ON l.initial_query_id = q.query_id
WHERE l.event_date >= yesterday() AND l.event_time >= now() - 600
  AND l.type = 'QueryFinish'
GROUP BY q.log_comment
ORDER BY q.log_comment;

DROP TABLE tab_bf_early_exit;
