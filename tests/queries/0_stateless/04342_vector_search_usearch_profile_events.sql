-- Tags: no-fasttest

-- `VectorScorer::scorePart` increments the same `USearchSearchCount`,
-- `USearchSearchVisitedMembers` and `USearchSearchComputedDistances`
-- profile events as the legacy `ORDER BY ... LIMIT` vector-index path
-- (`MergeTreeIndexConditionVectorSimilarity`), for both the plain and the
-- prefiltered (`WHERE`-driven) USearch search, so `system.query_log` does
-- not underreport table-function searches.

SET allow_experimental_search_topk_table_functions = 1;

DROP TABLE IF EXISTS tab;

CREATE TABLE tab
(
    id Int32,
    payload UInt32,
    vec Array(Float32),
    INDEX idx vec TYPE vector_similarity('hnsw', 'L2Distance', 2) GRANULARITY 100000000
)
ENGINE = MergeTree
ORDER BY id
SETTINGS min_bytes_for_wide_part = 0;

SYSTEM STOP MERGES tab;

INSERT INTO tab SELECT number, number % 2, [toFloat32(number), 0.0] FROM numbers(100);

SELECT '-- plain search';
SELECT id FROM vectorSearch(currentDatabase(), tab, idx, [0.0, 0.0], 3)
ORDER BY id
SETTINGS log_comment = '04342_usearch_profile_events_plain';

SELECT '-- filtered search';
SELECT id FROM vectorSearch(currentDatabase(), tab, idx, [0.0, 0.0], 3)
WHERE payload = 0
ORDER BY id
SETTINGS log_comment = '04342_usearch_profile_events_filtered';

SYSTEM FLUSH LOGS query_log;

SELECT '-- USearchSearch* events incremented (one row per query)';
SELECT
    ProfileEvents['USearchSearchCount'] > 0,
    ProfileEvents['USearchSearchVisitedMembers'] > 0,
    ProfileEvents['USearchSearchComputedDistances'] > 0
FROM system.query_log
WHERE current_database = currentDatabase()
  AND event_date >= yesterday() AND event_time >= now() - INTERVAL 10 MINUTE
  AND type = 'QueryFinish'
  AND log_comment LIKE '04342_usearch_profile_events_%'
ORDER BY log_comment;

DROP TABLE tab;
