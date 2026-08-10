-- Tags: no-fasttest

-- Plan 01 §"Tests": **regression test for the user's complaint about no
-- skip-index composition.** With PK `(date, id)` and a `WHERE date = today()`
-- filter, only the matching mark range must be scored — that is,
-- `ProfileEvents.SelectedMarks < SelectedMarksTotal` on the storage's
-- precursor `ReadFromMergeTree`. The scored-search storage base feeds
-- `query_info.filter_actions_dag` into the precursor through
-- `MergeTreeDataSelectExecutor` so PK index analysis fires before scoring.

SET allow_experimental_search_topk_table_functions = 1;

DROP TABLE IF EXISTS tab;

-- One mark per granule. `index_granularity = 1` so each row lands in its
-- own mark; the PK filter on `date` must prune marks individually.
CREATE TABLE tab
(
    date Date,
    id Int32,
    vec Array(Float32),
    INDEX idx vec TYPE vector_similarity('hnsw', 'L2Distance', 2) GRANULARITY 100000000
)
ENGINE = MergeTree
ORDER BY (date, id)
SETTINGS min_bytes_for_wide_part = 0, index_granularity = 1;

SYSTEM STOP MERGES tab;

-- Two rows on `today() - 1`, two rows on `today()`. PK pruning on
-- `date = today()` must drop the first two marks.
INSERT INTO tab VALUES
    (today() - 1, 0, [1.0, 0.0]),
    (today() - 1, 1, [1.1, 0.0]),
    (today(),     2, [1.0, 0.0]),
    (today(),     3, [1.1, 0.0]);

SELECT '-- ids returned: only today rows';
SELECT id
FROM vectorSearch(currentDatabase(), tab, idx, [1.0, 0.0], 5)
WHERE date = today()
ORDER BY id
SETTINGS log_comment = '04268_scored_search_base_pk_pruning';

SYSTEM FLUSH LOGS query_log;

SELECT '-- SelectedMarks < SelectedMarksTotal';
SELECT ProfileEvents['SelectedMarks'] < ProfileEvents['SelectedMarksTotal']
FROM system.query_log
WHERE current_database = currentDatabase()
  AND event_date >= yesterday() AND event_time >= now() - INTERVAL 10 MINUTE
  AND type = 'QueryFinish'
  AND log_comment = '04268_scored_search_base_pk_pruning';

DROP TABLE tab;
