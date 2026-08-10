-- Tags: no-fasttest

-- Plan 01 §"Tests": `StorageMergeTreeScoredSearchBase` honours partition
-- pruning. With a `WHERE` filter that selects a single partition out of
-- two, the precursor `ReadFromMergeTree` (driving both the bitmap subquery
-- and `ranges_in_data_parts`) must drop the unselected partition's parts —
-- `ProfileEvents.SelectedParts < SelectedPartsTotal`.

SET allow_experimental_search_topk_table_functions = 1;

DROP TABLE IF EXISTS tab;

CREATE TABLE tab
(
    id Int32,
    category String,
    vec Array(Float32),
    INDEX idx vec TYPE vector_similarity('hnsw', 'L2Distance', 2) GRANULARITY 100000000
)
ENGINE = MergeTree
PARTITION BY category
ORDER BY id
SETTINGS min_bytes_for_wide_part = 0;

SYSTEM STOP MERGES tab;

-- One part per partition.
INSERT INTO tab VALUES (0, 'a', [1.0, 0.0]), (1, 'a', [1.1, 0.0]);
INSERT INTO tab VALUES (2, 'b', [0.0, 1.0]), (3, 'b', [0.0, 1.1]);

SELECT '-- ids returned: only partition a';
SELECT id
FROM vectorSearch(currentDatabase(), tab, idx, [1.0, 0.0], 3)
WHERE category = 'a'
ORDER BY id
SETTINGS log_comment = '04267_scored_search_base_partition_pruning';

SYSTEM FLUSH LOGS query_log;

SELECT '-- SelectedParts < SelectedPartsTotal';
-- Selected one partition out of two => SelectedParts < SelectedPartsTotal.
SELECT ProfileEvents['SelectedParts'] < ProfileEvents['SelectedPartsTotal']
FROM system.query_log
WHERE current_database = currentDatabase()
  AND event_date >= yesterday() AND event_time >= now() - INTERVAL 10 MINUTE
  AND type = 'QueryFinish'
  AND log_comment = '04267_scored_search_base_partition_pruning';

DROP TABLE tab;
