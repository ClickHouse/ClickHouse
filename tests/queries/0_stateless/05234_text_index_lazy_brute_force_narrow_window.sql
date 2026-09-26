-- The brute-force lazy intersection (`intersectBruteForce`) scans every posting list after the first only over
-- the rows the previous ones wrote: the first cursor returns the range of rows it set, each following cursor
-- narrows it to the rows it incremented, and the final pass covers that range alone. Packed blocks outside the
-- range are neither examined nor decoded. Before, they were skipped one by one after a scan of their all-zero
-- output region, counted by `TextIndexLazyBlocksSkippedResolved`; with a sparse token clustered inside every
-- mark, that counter (and the segment-level one) must now stay at zero. Every result is compared with a plain
-- column scan, which also checks that the counters outside the narrowed range are zeroed by the final pass.

SET enable_full_text_index = 1;
SET use_skip_indexes = 1;
SET use_skip_indexes_on_data_read = 1;
SET query_plan_direct_read_from_text_index = 1;
SET query_plan_optimize_count_from_text_index = 0;
SET use_query_condition_cache = 0;
SET merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 0;

DROP TABLE IF EXISTS tab_bf_narrow;

-- One index granule covers the whole part, so the skip index prunes no mark and every mark of the part
-- reaches the intersection with all posting lists.
CREATE TABLE tab_bf_narrow
(
    id UInt64,
    s String,
    INDEX idx s TYPE text(tokenizer = splitByNonAlpha, posting_list_codec = 'bitpacking', posting_list_block_size = 1024) GRANULARITY 100000000
)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';

-- 25 marks of 8192 rows. `posting_list_block_size = 1024` gives every token but `fsingle` several segments
-- of 8 packed blocks (128 rows each). The cursors are sorted by ascending cardinality.
--   aall    : every row                                      -> dense segments, padded without decoding
--   bhalf   : every even row                                 -> 32 packed blocks per mark
--   cburst  : the first 300 rows of every mark               -> sparsest compressed list, clustered at the mark start
--   dmid    : rows 4000..4249 of every mark                  -> clustered in the middle of the mark
--   epart   : rows 100..199 of every mark, plus every third row from row 300 on
--                                                            -> inside the rows of `cburst` it covers only 100..199
--   fsingle : the first 40 rows of every mark                -> 1000 rows in a single segment, which the analyzer
--                                                               reads eagerly and hands to the cursors as a flat array
INSERT INTO tab_bf_narrow
SELECT number,
    concat('aall',
        if(number % 2 = 0, ' bhalf', ''),
        if(number % 8192 < 300, ' cburst', ''),
        if(number % 8192 >= 4000 AND number % 8192 < 4250, ' dmid', ''),
        if((number % 8192 >= 100 AND number % 8192 < 200) OR (number % 8192 >= 300 AND number % 3 = 0), ' epart', ''),
        if(number % 8192 < 40, ' fsingle', ''))
FROM numbers(204800)
SETTINGS max_insert_threads = 1, max_insert_block_size = 1000000, min_insert_block_size_rows = 1000000, min_insert_block_size_bytes = 0;

SELECT 'parts', count() FROM system.parts WHERE database = currentDatabase() AND table = 'tab_bf_narrow' AND active;

SELECT 'Ground truth (no index)';
SET use_skip_indexes = 0;
SELECT 'all bhalf cburst', count(), sum(id) FROM tab_bf_narrow WHERE hasAllTokens(s, ['bhalf', 'cburst']);
SELECT 'all bhalf dmid', count(), sum(id) FROM tab_bf_narrow WHERE hasAllTokens(s, ['bhalf', 'dmid']);
SELECT 'all bhalf cburst epart', count(), sum(id) FROM tab_bf_narrow WHERE hasAllTokens(s, ['bhalf', 'cburst', 'epart']);
SELECT 'all aall cburst epart', count(), sum(id) FROM tab_bf_narrow WHERE hasAllTokens(s, ['aall', 'cburst', 'epart']);
SELECT 'all bhalf fsingle', count(), sum(id) FROM tab_bf_narrow WHERE hasAllTokens(s, ['bhalf', 'fsingle']);
SELECT 'all cburst fsingle', count(), sum(id) FROM tab_bf_narrow WHERE hasAllTokens(s, ['cburst', 'fsingle']);
SELECT 'all cburst dmid', count(), sum(id) FROM tab_bf_narrow WHERE hasAllTokens(s, ['cburst', 'dmid']);

SELECT 'Lazy, brute-force intersection';
SET use_skip_indexes = 1;
SET text_index_posting_list_apply_mode = 'lazy';
SET text_index_postings_intersection_algorithm = 'bruteforce';

-- `cburst` sets 300 rows at the start of every mark: `bhalf` decodes the one or two blocks covering them.
SELECT 'all bhalf cburst', count(), sum(id) FROM tab_bf_narrow WHERE hasAllTokens(s, ['bhalf', 'cburst'])
    SETTINGS log_comment = '05234_bf_bhalf_cburst';

-- The same with the rows in the middle of the mark: blocks on both sides stay untouched.
SELECT 'all bhalf dmid', count(), sum(id) FROM tab_bf_narrow WHERE hasAllTokens(s, ['bhalf', 'dmid'])
    SETTINGS log_comment = '05234_bf_bhalf_dmid';

-- `epart` narrows the range of `cburst` to rows 100..199, so `bhalf` scans those alone, and the counters
-- of rows 0..99 and 200..299 (set by `cburst`, incremented by nobody else) are zeroed by the final pass.
SELECT 'all bhalf cburst epart', count(), sum(id) FROM tab_bf_narrow WHERE hasAllTokens(s, ['bhalf', 'cburst', 'epart'])
    SETTINGS log_comment = '05234_bf_bhalf_cburst_epart';

-- The dense list is padded over the narrowed range only.
SELECT 'all aall cburst epart', count(), sum(id) FROM tab_bf_narrow WHERE hasAllTokens(s, ['aall', 'cburst', 'epart'])
    SETTINGS log_comment = '05234_bf_aall_cburst_epart';

-- The flat array of `fsingle` goes first and narrows the range to the first 40 rows of every mark.
SELECT 'all bhalf fsingle', count(), sum(id) FROM tab_bf_narrow WHERE hasAllTokens(s, ['bhalf', 'fsingle'])
    SETTINGS log_comment = '05234_bf_bhalf_fsingle';

SELECT 'all cburst fsingle', count(), sum(id) FROM tab_bf_narrow WHERE hasAllTokens(s, ['cburst', 'fsingle'])
    SETTINGS log_comment = '05234_bf_cburst_fsingle';

-- Disjoint rows: `dmid` finds nothing in the range of `cburst`, the intersection stops early in every mark.
SELECT 'all cburst dmid', count(), sum(id) FROM tab_bf_narrow WHERE hasAllTokens(s, ['cburst', 'dmid'])
    SETTINGS log_comment = '05234_bf_cburst_dmid';

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
      AND log_comment LIKE '05234_bf_%'
)
SELECT
    q.log_comment,
    sum(ProfileEvents['TextIndexLazyBruteForceIntersections']) > 0 AS brute_force,
    sum(ProfileEvents['TextIndexLazyBruteForceEarlyExits']) > 0 AS early_exits,
    sum(ProfileEvents['TextIndexLazyBlocksSkippedResolved']) AS blocks_skipped_resolved,
    sum(ProfileEvents['TextIndexLazySegmentsSkippedResolved']) AS segments_skipped_resolved
FROM system.query_log AS l
INNER JOIN initial_queries AS q ON l.initial_query_id = q.query_id
WHERE l.event_date >= yesterday() AND l.event_time >= now() - 600
  AND l.type = 'QueryFinish'
GROUP BY q.log_comment
ORDER BY q.log_comment;

DROP TABLE tab_bf_narrow;
