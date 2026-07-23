-- Tags: long, no-parallel, no-parallel-replicas
-- Tag no-parallel: Messes with internal cache
-- Tag long: needs ~1M rows for the QCC to populate.
--
-- The query condition cache (QCC) for `ORDER BY ... LIMIT n` (TopK dynamic filtering) reads is
-- gated behind the `use_query_condition_cache_for_top_k` setting and is DISABLED by default.
-- This test asserts the default-off contract at every gated touch point:
--   * a TopK read must not reuse an entry primed by a plain `SELECT ... WHERE` with the same
--     predicate (no predicate-only reuse path);
--   * a TopK read must not write any QCC entry (neither the WHERE write in
--     `updateQueryConditionCache`, nor index-analysis exclusions in `selectRangesToRead`,
--     nor row-level entries from the reader);
--   * plain `WHERE` queries keep using the QCC as usual.
--
-- The behaviour is asserted from the read side via the `QueryConditionCacheHits` profile event
-- and from the write side via `count()` on `system.query_condition_cache` after running only
-- TopK queries on a cleared cache.

SET allow_experimental_analyzer = 1;
SET use_query_condition_cache = 1;
-- Pin the gate to its default value: this test covers the default-off contract.
SET use_query_condition_cache_for_top_k = 0;
SET use_top_k_dynamic_filtering = 1;
SET use_skip_indexes_for_top_k = 1;
SET query_plan_max_limit_for_top_k_optimization = 1000;
SET optimize_move_to_prewhere = 0;
SET enable_parallel_replicas = 0;
SET automatic_parallel_replicas_mode = 0;
SET parallel_replicas_local_plan = 1;
SET max_threads = 1;

DROP TABLE IF EXISTS tab;

CREATE TABLE tab (id UInt32, v1 UInt32, v2 UInt32) ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 64,
         min_bytes_for_wide_part = 0,
         min_bytes_for_full_part_storage = 0,
         add_minmax_index_for_numeric_columns = 0;

INSERT INTO tab SELECT rand(), number, number FROM numbers(1_000_000);

SELECT '--- A TopK read must not reuse a plain WHERE entry when the gate is off';
SYSTEM CLEAR QUERY CONDITION CACHE;

-- Prime the cache with a plain WHERE. First touch of this predicate: cache miss, all granules read.
SELECT v1 FROM tab WHERE v2 = 10000 FORMAT Null SETTINGS log_comment = '04628_prime';
-- TopK read of the same predicate with the gate off: must not consult the predicate-only entry.
SELECT v1 FROM tab WHERE v2 = 10000 ORDER BY v1 ASC LIMIT 5 FORMAT Null SETTINGS log_comment = '04628_topk_after_prime';
-- A second plain WHERE still reuses its own entry (the QCC is only off for TopK reads).
SELECT v1 FROM tab WHERE v2 = 10000 FORMAT Null SETTINGS log_comment = '04628_plain_reuse';

SYSTEM FLUSH LOGS query_log;

-- Column: (any QCC hit). Expected: prime = 0, topk-after-prime = 0, plain-reuse = 1.
SELECT
    log_comment,
    ProfileEvents['QueryConditionCacheHits'] > 0
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600
    AND type = 'QueryFinish'
    AND current_database = currentDatabase()
    AND log_comment IN ('04628_prime', '04628_topk_after_prime', '04628_plain_reuse')
ORDER BY event_time_microseconds;

SELECT '--- TopK still returns the planted row';
SELECT v1 FROM tab WHERE v2 = 10000 ORDER BY v1 ASC LIMIT 5;

SELECT '--- A TopK read must not write any QCC entry when the gate is off';
SYSTEM CLEAR QUERY CONDITION CACHE;

-- Run the TopK query twice on a cleared cache: no entry may be written, so the second run
-- must not hit either.
SELECT v1 FROM tab WHERE v2 = 10000 ORDER BY v1 ASC LIMIT 5 FORMAT Null SETTINGS log_comment = '04628_topk_write_1';
SELECT v1 FROM tab WHERE v2 = 10000 ORDER BY v1 ASC LIMIT 5 FORMAT Null SETTINGS log_comment = '04628_topk_write_2';

-- Expected: 0 entries — the TopK read wrote nothing under any key (salted or plain).
SELECT count() FROM system.query_condition_cache;

SYSTEM FLUSH LOGS query_log;

-- Column: (any QCC hit). Expected: 0 for both runs.
SELECT
    log_comment,
    ProfileEvents['QueryConditionCacheHits'] > 0
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600
    AND type = 'QueryFinish'
    AND current_database = currentDatabase()
    AND log_comment IN ('04628_topk_write_1', '04628_topk_write_2')
ORDER BY event_time_microseconds;

DROP TABLE tab;
