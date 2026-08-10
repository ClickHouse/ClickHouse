-- Tags: no-fasttest

-- Plan 01a §"No-WHERE case": for `vectorSearch` with no WHERE clause,
-- the storage base's `read()` skips constructing the
-- `DelayedCreatingBitmapsStep` entirely. The per-part `ScorerSource`s
-- run with `prefilter = nullptr` and the bitmap subquery is never
-- planted. This is Critic 1.2's obligation 5 — pinned here once a
-- concrete `StorageMergeTreeScoredSearchBase` subclass exists.

SET allow_experimental_search_topk_table_functions = 1;

DROP TABLE IF EXISTS tab;

CREATE TABLE tab
(
    id Int32,
    vec Array(Float32),
    INDEX idx vec TYPE vector_similarity('hnsw', 'L2Distance', 2) GRANULARITY 100000000
)
ENGINE = MergeTree
ORDER BY id
SETTINGS min_bytes_for_wide_part = 0;

SYSTEM STOP MERGES tab;

INSERT INTO tab VALUES (0, [1.0, 0.0]), (1, [1.1, 0.0]), (2, [0.0, 1.0]);

SELECT '-- no-WHERE: no bitmap gating in pipeline';
-- `DelayedCreatingBitmaps` is always planted on the plan tree (its presence
-- is what allows analyzer-mode `applyFilters` to deposit a subquery later)
-- but with no WHERE it acts as a passthrough — no `DelayedPorts` processor
-- (`DelayedPortsProcessor`) is added to the main pipeline, which is what
-- `addPipelineBefore` would insert to gate the scorer on the bitmap subquery.
SELECT count() = 0
FROM (EXPLAIN PIPELINE SELECT id FROM vectorSearch(currentDatabase(), tab, idx, [1.0, 0.0], 3))
WHERE explain LIKE '%DelayedPorts%';

SELECT '-- with-WHERE: bitmap gating is planted';
SELECT count() > 0
FROM (EXPLAIN PIPELINE SELECT id FROM vectorSearch(currentDatabase(), tab, idx, [1.0, 0.0], 3) WHERE id >= 0)
WHERE explain LIKE '%DelayedPorts%';

SELECT '-- no-WHERE: result is computed without prefilter';
SELECT id
FROM vectorSearch(currentDatabase(), tab, idx, [1.0, 0.0], 3)
ORDER BY id
SETTINGS log_comment = '04272_scored_search_base_no_where_no_prefilter';

SYSTEM FLUSH LOGS query_log;

SELECT '-- no-WHERE: prefilter bitmap rows == 0';
-- With no WHERE, `BuildBitmapsTransform` is never created so the
-- bitmap-rows counter must be zero.
SELECT ProfileEvents['ScoredSearchPrefilterBitmapRows']
FROM system.query_log
WHERE current_database = currentDatabase()
  AND event_date >= yesterday() AND event_time >= now() - INTERVAL 10 MINUTE
  AND type = 'QueryFinish'
  AND log_comment = '04272_scored_search_base_no_where_no_prefilter';

DROP TABLE tab;
