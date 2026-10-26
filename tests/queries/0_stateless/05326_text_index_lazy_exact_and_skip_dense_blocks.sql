-- The brute-force lazy intersection (`intersectBruteForce`) counts per row how many posting lists contain it.
-- After k lists, a counter equal to k marks a row present in all of them, so a packed block of the next list
-- is skipped when no counter in its rows equals k, even when other counters there are not zero
-- (`TextIndexLazyBlocksSkippedResolved`). A packed block of consecutive row ids is padded as a whole instead
-- of being decoded (`TextIndexLazyBlocksSkippedDense`). Every result is compared with a plain column scan.

SET enable_full_text_index = 1;
SET use_skip_indexes = 1;
SET use_skip_indexes_on_data_read = 1;
SET query_plan_direct_read_from_text_index = 1;
SET query_plan_optimize_count_from_text_index = 0;
SET use_query_condition_cache = 0;
SET merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 0;
SET max_block_size = 8192;

DROP TABLE IF EXISTS tab_exact_skip;

-- One index granule covers the whole part, so every mark reaches the intersection with all posting lists.
CREATE TABLE tab_exact_skip
(
    id UInt64,
    s String,
    INDEX idx s TYPE text(tokenizer = splitByNonAlpha, posting_list_codec = 'bitpacking', posting_list_block_size = 1024) GRANULARITY 100000000
)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';

-- 25 marks of 8192 rows. The cursors are sorted by ascending cardinality.
--   alead : every 64th row                                 -> 128 rows per mark
--   bmid  : every odd row, plus the first 512 rows of every mark
--           -> holds the rows of `alead` only in the first 512 rows of a mark, but rows in every packed block
--   clast : every row not divisible by 3                    -> packed blocks of non-consecutive rows
--   druns : the first 1000 rows of every mark               -> packed blocks of consecutive rows
INSERT INTO tab_exact_skip
SELECT number,
    concat('x',
        if(number % 64 = 0, ' alead', ''),
        if(number % 2 = 1 OR number % 8192 < 512, ' bmid', ''),
        if(number % 3 != 0, ' clast', ''),
        if(number % 8192 < 1000, ' druns', ''))
FROM numbers(204800)
SETTINGS max_insert_threads = 1, max_insert_block_size = 1000000, min_insert_block_size_rows = 1000000, min_insert_block_size_bytes = 0;

SELECT 'parts', count() FROM system.parts WHERE database = currentDatabase() AND table = 'tab_exact_skip' AND active;

SELECT 'Ground truth (no index)';
SET use_skip_indexes = 0;
SELECT 'all alead bmid clast', count(), sum(id) FROM tab_exact_skip WHERE hasAllTokens(s, ['alead', 'bmid', 'clast']);
SELECT 'all druns clast', count(), sum(id) FROM tab_exact_skip WHERE hasAllTokens(s, ['druns', 'clast']);
SELECT 'druns', count(), sum(id) FROM tab_exact_skip WHERE hasToken(s, 'druns');
SELECT 'any druns alead', count(), sum(id) FROM tab_exact_skip WHERE hasAnyTokens(s, ['druns', 'alead']);

SELECT 'Lazy';
SET use_skip_indexes = 1;
SET text_index_posting_list_apply_mode = 'lazy';

-- After `alead` and `bmid`, only the rows of `alead` in the first 512 rows of a mark have a counter of 2,
-- so the packed blocks of `clast` in the rest of the mark are skipped, although `bmid` set counters there.
SELECT 'all alead bmid clast', count(), sum(id) FROM tab_exact_skip WHERE hasAllTokens(s, ['alead', 'bmid', 'clast'])
    SETTINGS text_index_postings_intersection_algorithm = 'bruteforce', log_comment = '05326_q_bf_exact_skip';

SELECT 'all alead bmid clast', count(), sum(id) FROM tab_exact_skip WHERE hasAllTokens(s, ['alead', 'bmid', 'clast'])
    SETTINGS text_index_postings_intersection_algorithm = 'leapfrog', log_comment = '05326_q_leapfrog';

-- `druns` sets its rows from packed blocks of consecutive row ids without decoding them.
SELECT 'all druns clast', count(), sum(id) FROM tab_exact_skip WHERE hasAllTokens(s, ['druns', 'clast'])
    SETTINGS text_index_postings_intersection_algorithm = 'bruteforce', log_comment = '05326_q_bf_dense_blocks';

SELECT 'druns', count(), sum(id) FROM tab_exact_skip WHERE hasToken(s, 'druns')
    SETTINGS log_comment = '05326_q_or_dense_blocks';

SELECT 'any druns alead', count(), sum(id) FROM tab_exact_skip WHERE hasAnyTokens(s, ['druns', 'alead'])
    SETTINGS log_comment = '05326_q_any_dense_blocks';

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
      AND startsWith(log_comment, '05326_q_')
)
SELECT
    q.log_comment,
    sum(ProfileEvents['TextIndexLazyBruteForceIntersections']) > 0 AS brute_force,
    sum(ProfileEvents['TextIndexLazyLeapfrogIntersections']) > 0 AS leapfrog,
    sum(ProfileEvents['TextIndexLazyBlocksSkippedResolved']) > 0 AS blocks_skipped_resolved,
    sum(ProfileEvents['TextIndexLazyBlocksSkippedDense']) > 0 AS blocks_skipped_dense
FROM system.query_log AS l
INNER JOIN initial_queries AS q ON l.initial_query_id = q.query_id
WHERE l.event_date >= yesterday() AND l.event_time >= now() - 600
  AND l.type = 'QueryFinish'
GROUP BY q.log_comment
ORDER BY q.log_comment;

DROP TABLE tab_exact_skip;
