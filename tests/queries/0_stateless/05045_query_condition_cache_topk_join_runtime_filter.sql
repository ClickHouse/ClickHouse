-- Tags: long, no-parallel, no-parallel-replicas
-- Tag no-parallel: Messes with internal cache
-- Tag long: needs ~1M rows for the QCC to populate (a granule-spanning chunk must be
--   fully filtered before the LIMIT cancels the pipeline).
--
-- TopK + lazy materialization + join runtime filter: a join runtime filter
-- (`__applyFilter`) changes which rows reach the sorter and therefore the running TopK
-- threshold, but its per-execution contents are not part of the query condition cache
-- key. So when a runtime filter sits above (or is merged into) a read, no WHERE-side
-- QCC entry may be written or reused for that read — including by the re-tagging
-- `updateQueryConditionCache` walk that runs after lazy materialization replaces
-- `FilterStep`s (it must not recreate a key that the runtime-filter cleanup dropped).
--
-- In plans constructible today the runtime filter is merged into the read's filter
-- (`... AND RF1(...)`) and TopK is not stamped through the join, so the sound behavior
-- is: zero cache entries, zero hits. The single-table control shows the same
-- TopK + lazy materialization shape does populate and reuse the cache, so the zeros
-- below assert real suppression rather than a switched-off cache.

SET allow_experimental_analyzer = 1;
SET use_query_condition_cache = 1;
SET use_top_k_dynamic_filtering = 1;
SET use_skip_indexes_for_top_k = 1;
SET query_plan_max_limit_for_top_k_optimization = 1000;
SET query_plan_optimize_lazy_materialization = 1;
SET query_plan_max_limit_for_lazy_materialization = 1000;
SET optimize_move_to_prewhere = 0;
SET enable_join_runtime_filters = 1;
SET enable_parallel_replicas = 0;
SET automatic_parallel_replicas_mode = 0;
SET parallel_replicas_local_plan = 1;
SET max_threads = 1;

DROP TABLE IF EXISTS tab;
DROP TABLE IF EXISTS dim;

CREATE TABLE tab (id UInt32, v1 UInt32, v2 UInt32, extra UInt32) ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 64,
         min_bytes_for_wide_part = 0,
         min_bytes_for_full_part_storage = 0,
         add_minmax_index_for_numeric_columns = 0;

INSERT INTO tab SELECT rand(), number, number, number * 2 FROM numbers(1_000_000);

CREATE TABLE dim (id UInt32) ENGINE = MergeTree ORDER BY id;

INSERT INTO dim SELECT rand() FROM numbers(10_000);

SELECT '--- Control: the same single-table TopK + lazy materialization shape populates and reuses the QCC';
SYSTEM CLEAR QUERY CONDITION CACHE;

SELECT v1, extra FROM tab WHERE v2 = 10000 ORDER BY v1 ASC LIMIT 5 FORMAT Null SETTINGS log_comment = '05045_control_cold';
SELECT v1, extra FROM tab WHERE v2 = 10000 ORDER BY v1 ASC LIMIT 5 FORMAT Null SETTINGS log_comment = '05045_control_reuse';

SELECT count() > 0 FROM system.query_condition_cache;

SELECT '--- The join plan carries a runtime filter into the TopK-shaped read';

-- The runtime filter is merged into the read's filter as an `RF...` conjunct, and no
-- `__topKFilter` is stamped through the join (`topKThroughJoin` and `joinRuntimeFilter`
-- target disjoint join shapes).
SELECT
    (countIf(explain LIKE '%BuildRuntimeFilter%') > 0)
AND (countIf(explain LIKE '%__topKFilter%') = 0)
FROM (
    EXPLAIN actions = 1
    SELECT t.v1, t.extra FROM tab t INNER JOIN dim d ON t.id = d.id WHERE t.v2 < 500000 ORDER BY t.v1 ASC LIMIT 5);

SELECT '--- With a join runtime filter no WHERE-side QCC entry is written or reused';
SYSTEM CLEAR QUERY CONDITION CACHE;

SELECT t.v1, t.extra FROM tab t INNER JOIN dim d ON t.id = d.id WHERE t.v2 < 500000 ORDER BY t.v1 ASC LIMIT 5 FORMAT Null SETTINGS log_comment = '05045_join_cold';
SELECT t.v1, t.extra FROM tab t INNER JOIN dim d ON t.id = d.id WHERE t.v2 < 500000 ORDER BY t.v1 ASC LIMIT 5 FORMAT Null SETTINGS log_comment = '05045_join_reuse';

-- No entry may exist: the filter above the read carries the runtime filter, so its
-- condition is not deterministic across executions.
SELECT count() FROM system.query_condition_cache;

SYSTEM FLUSH LOGS query_log;

-- Columns: (any QCC hit). Expected: control_cold = 0, control_reuse = 1 (the cache works
-- in this shape), join_cold = 0, join_reuse = 0 (nothing threshold-dependent is reused).
SELECT
    log_comment,
    ProfileEvents['QueryConditionCacheHits'] > 0
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600
    AND type = 'QueryFinish'
    AND current_database = currentDatabase()
    AND log_comment IN ('05045_control_cold', '05045_control_reuse', '05045_join_cold', '05045_join_reuse')
ORDER BY event_time_microseconds;

DROP TABLE tab;
DROP TABLE dim;
