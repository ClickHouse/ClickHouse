-- Tags: long, no-parallel, no-parallel-replicas
-- Tag no-parallel: Messes with internal cache
-- Tag long: needs ~1M rows for the QCC to populate.
--
-- A `SELECT ... WHERE <predicate> ORDER BY ... LIMIT n` query selected for TopK dynamic
-- filtering probes the query condition cache (QCC) under the bare `<predicate>` hash in
-- addition to its TopK-salted hash, so it reuses an entry primed by a plain
-- `SELECT ... WHERE <predicate>`. The reverse must not happen: a plain `WHERE` query must
-- not read a TopK-salted entry.
--
-- The behaviour is asserted from the read side via the `QueryConditionCacheHits` profile
-- event (and the dropped-granule effect via `SelectedMarks` < `SelectedMarksTotal`), rather
-- than from `count()` on `system.query_condition_cache`: the entry count cannot distinguish a
-- genuine read-side reuse from a query that simply fails to write a new entry, so it stays
-- green even if the `topk_reuse_predicate_only_hash` lookup is removed.

SET allow_experimental_analyzer = 1;
SET use_query_condition_cache = 1;
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

SELECT '--- Forward: plain WHERE primes an entry that the TopK read reuses';
SYSTEM CLEAR QUERY CONDITION CACHE;

-- Prime the cache with a plain WHERE. First touch of this predicate: cache miss, all granules read.
SELECT v1 FROM tab WHERE v2 = 10000 FORMAT Null SETTINGS log_comment = '04539_fwd_prime';
-- TopK read of the same predicate: reuses the predicate-only entry, so it hits and drops granules.
SELECT v1 FROM tab WHERE v2 = 10000 ORDER BY v1 ASC LIMIT 5 FORMAT Null SETTINGS log_comment = '04539_fwd_topk';

SYSTEM FLUSH LOGS query_log;

-- Columns: (any QCC hit), (granules skipped). Expected: prime = 0 0, topk-reuse = 1 1.
SELECT
    log_comment,
    ProfileEvents['QueryConditionCacheHits'] > 0,
    toInt32(ProfileEvents['SelectedMarks']) < toInt32(ProfileEvents['SelectedMarksTotal'])
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600
    AND type = 'QueryFinish'
    AND current_database = currentDatabase()
    AND log_comment IN ('04539_fwd_prime', '04539_fwd_topk')
ORDER BY event_time_microseconds;

SELECT '--- TopK still returns the planted row';
SELECT v1 FROM tab WHERE v2 = 10000 ORDER BY v1 ASC LIMIT 5;

SELECT '--- Reverse: a TopK-salted entry must not be read by a plain WHERE';
SYSTEM CLEAR QUERY CONDITION CACHE;

-- Prime the cache with a TopK query only: writes a TopK-salted entry.
SELECT v1 FROM tab WHERE v2 = 10000 ORDER BY v1 ASC LIMIT 5 FORMAT Null SETTINGS log_comment = '04539_rev_topk';
-- Plain WHERE must not reuse the TopK-salted entry: cache miss, all granules read.
SELECT v1 FROM tab WHERE v2 = 10000 FORMAT Null SETTINGS log_comment = '04539_rev_plain';

SYSTEM FLUSH LOGS query_log;

-- Columns: (any QCC hit), (granules skipped). Expected: plain-after-topk = 0 0.
SELECT
    log_comment,
    ProfileEvents['QueryConditionCacheHits'] > 0,
    toInt32(ProfileEvents['SelectedMarks']) < toInt32(ProfileEvents['SelectedMarksTotal'])
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600
    AND type = 'QueryFinish'
    AND current_database = currentDatabase()
    AND log_comment = '04539_rev_plain'
ORDER BY event_time_microseconds;

SELECT '--- Plain WHERE still returns the planted row';
SELECT v1 FROM tab WHERE v2 = 10000;

DROP TABLE tab;
