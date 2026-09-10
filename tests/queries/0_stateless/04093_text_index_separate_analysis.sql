-- Tags: no-parallel-replicas
-- Tag no-parallel-replicas -- direct read is not compatible with parallel replicas

-- Test that text index analysis is separated from MergeTreeReaderTextIndex:
-- 1) Direct read from text index works even when use_skip_indexes_on_data_read = 0
-- 2) Two text indexes with different selectivities both filter marks correctly

SET log_queries = 1;
SET enable_analyzer = 0; -- Consistent EXPLAIN output
SET optimize_move_to_prewhere = 1;
SET query_plan_optimize_prewhere = 1;
SET query_plan_direct_read_from_text_index = 1;
SET use_query_condition_cache = 0;
SET query_plan_remove_unused_columns = 1;
SET merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 0;

DROP TABLE IF EXISTS tab;

CREATE TABLE tab
(
    id UInt64,
    category String,
    message String,
    INDEX idx_category(category) TYPE text(tokenizer = splitByNonAlpha),
    INDEX idx_message(message) TYPE text(tokenizer = splitByNonAlpha)
)
ENGINE = MergeTree()
ORDER BY id
SETTINGS index_granularity = 2, index_granularity_bytes = '10Mi';

-- 10 rows, 5 granules of 2 rows each.
-- `category` column: 'common' appears in all rows (low selectivity index).
-- `message` column: 'xyzspecial' appears only in rows 0 and 1, i.e. granule 0 (high selectivity index).
INSERT INTO tab VALUES
    (0, 'common', 'xyzspecial hello'),
    (1, 'common', 'xyzspecial world'),
    (2, 'common', 'frequent one'),
    (3, 'common', 'frequent two'),
    (4, 'common', 'frequent three'),
    (5, 'common', 'frequent four'),
    (6, 'common', 'frequent five'),
    (7, 'common', 'frequent six'),
    (8, 'common', 'frequent seven'),
    (9, 'common', 'frequent eight');

----------------------------------------------------------------------
SELECT '--- Part 1: Direct read works with use_skip_indexes_on_data_read = 0';
----------------------------------------------------------------------

-- Direct read should produce correct results even when skip indexes are analyzed pre-execution.
SELECT id, message FROM tab
WHERE hasAllTokens(message, 'xyzspecial')
ORDER BY id
SETTINGS use_skip_indexes_on_data_read = 0;

-- Two indexes combined: `category` has 'common' in all rows (not selective),
-- `message` has 'xyzspecial' in 2 rows. The result must be correct.
SELECT id FROM tab
WHERE hasAllTokens(category, 'common') AND hasAllTokens(message, 'xyzspecial')
ORDER BY id
SETTINGS use_skip_indexes_on_data_read = 0;

----------------------------------------------------------------------
SELECT '--- Part 2: EXPLAIN indexes shows granule filtering (use_skip_indexes_on_data_read = 0)';
----------------------------------------------------------------------

-- With use_skip_indexes_on_data_read = 0, text indexes are analyzed pre-execution
-- and should appear in EXPLAIN indexes output.
-- The high-selectivity idx_message should drop most granules.
SELECT trimLeft(explain) FROM (
    EXPLAIN indexes = 1, actions = 1
    SELECT count() FROM tab
    WHERE hasAllTokens(message, 'xyzspecial')
    SETTINGS use_skip_indexes_on_data_read = 0
)
WHERE explain LIKE '%Name:%' OR explain LIKE '%Description:%' OR explain LIKE '%Parts:%' OR explain LIKE '%Granules:%' OR explain LIKE '%INPUT%\_\_text_index%';

----------------------------------------------------------------------
SELECT '--- Part 3: Both indexes filter (use_skip_indexes_on_data_read = 0)';
----------------------------------------------------------------------

-- Both indexes applied: idx_category matches all (5/5 granules), idx_message matches 1/5 granules.
-- Net result: 1 granule selected.
SELECT trimLeft(explain) FROM (
    EXPLAIN indexes = 1, actions = 1
    SELECT count() FROM tab
    WHERE hasAllTokens(category, 'common') AND hasAllTokens(message, 'xyzspecial')
    SETTINGS use_skip_indexes_on_data_read = 0
)
WHERE explain LIKE '%Name:%' OR explain LIKE '%Description:%' OR explain LIKE '%Parts:%' OR explain LIKE '%Granules:%' OR explain LIKE '%INPUT%\_\_text_index%';

----------------------------------------------------------------------
SELECT '--- Part 4: SelectedMarks profile events';
----------------------------------------------------------------------

-- High-selectivity query: only 1 out of 5 granules should be selected.
SELECT count() FROM tab
WHERE hasAllTokens(message, 'xyzspecial')
SETTINGS log_comment = 'text_idx_separate_analysis_1';

-- Both indexes: same result, 1 granule.
SELECT count() FROM tab
WHERE hasAllTokens(category, 'common') AND hasAllTokens(message, 'xyzspecial')
SETTINGS log_comment = 'text_idx_separate_analysis_2';

SYSTEM FLUSH LOGS query_log;

-- SelectedMarks should be 1 (only the granule containing 'xyzspecial')
SELECT ProfileEvents['SelectedMarks'], read_rows FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = 'text_idx_separate_analysis_1';

SELECT ProfileEvents['SelectedMarks'], read_rows FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = 'text_idx_separate_analysis_2';

DROP TABLE tab;
