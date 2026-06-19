-- Tags: no-parallel-replicas

-- The build-side choice ('auto') uses the runtime hash-table-size cache (HashTablesStatistics):
-- a side whose metadata estimate is only an upper bound (a residual filter the index cannot use)
-- can get a much smaller cached size from a previous build, which the optimizer then uses as a
-- heuristic point estimate. This test exercises that path (the other build-side tests disable the
-- cache for determinism).
--
-- `f` has 1000000 rows; `WHERE g = 0` keeps 1000 of them but the index cannot prune on `g`, so
-- the scan estimate is the upper bound 1000000. Against `u` (exact 50000) that bound does not
-- prove `f` is smaller, so without a cached hint `f` would not be swapped onto the build side.
-- The first query forces `f` to the build side, recording its actual size (1000) in the cache;
-- the second ('auto') query then reuses that hint and builds on `f`.

SET enable_analyzer = 1;
SET use_statistics = 0;
SET enable_join_runtime_filters = 0;
SET query_plan_optimize_join_order_limit = 10;
SET query_plan_optimize_join_order_randomize = 0;
-- Enable the runtime hash-table-size cache (the point of this test) and make sure it is used.
SET collect_hash_table_stats_during_joins = 1;
SET use_hash_table_stats_for_join_reordering = 1;
SET max_size_to_preallocate_for_joins = 1000000000;

DROP TABLE IF EXISTS f;
DROP TABLE IF EXISTS u;

CREATE TABLE f (k Int32, g Int32) ENGINE = MergeTree ORDER BY k;
INSERT INTO f SELECT number, number % 1000 FROM numbers(1000000);

CREATE TABLE u (k Int32) ENGINE = MergeTree ORDER BY k;
INSERT INTO u SELECT number FROM numbers(50000);

-- Prime the cache: force the filtered `f` subquery onto the build side so its actual size is recorded.
SELECT * FROM (SELECT k FROM f WHERE g = 0) AS ff JOIN u ON ff.k = u.k
SETTINGS query_plan_join_swap_table = 'true' FORMAT Null;

-- Now with 'auto': the cached hint (1000) makes `f` the build side, even though its scan estimate
-- is only the upper bound 1000000.
SELECT * FROM (SELECT k FROM f WHERE g = 0) AS ff JOIN u ON ff.k = u.k
SETTINGS query_plan_join_swap_table = 'auto', log_comment = '04357_join_choose_build_table_cached_hint' FORMAT Null;

SYSTEM FLUSH LOGS query_log;

-- The filtered `f` (~1000 rows) must be the build side via the cached hint; `u` (50000) the probe.
SELECT
    if(ProfileEvents['JoinBuildTableRowCount'] BETWEEN 900 AND 1100, 'ok', format('fail({}): build={}', query_id, ProfileEvents['JoinBuildTableRowCount'])),
    if(ProfileEvents['JoinProbeTableRowCount'] BETWEEN 49000 AND 51000, 'ok', format('fail({}): probe={}', query_id, ProfileEvents['JoinProbeTableRowCount']))
FROM system.query_log
WHERE type = 'QueryFinish' AND event_date >= yesterday() AND event_time >= now() - 600
  AND query_kind = 'Select' AND current_database = currentDatabase()
  AND log_comment = '04357_join_choose_build_table_cached_hint'
ORDER BY event_time DESC
LIMIT 1;

-- A cached size must NOT anchor the upper-bound swap, because it is process-global and can be
-- stale-high relative to the CURRENT data even when it does not exceed the scan upper bound. Here
-- `tag = 1` matches all 1000000 rows on the first run, so the cache records 1000000; the data then
-- changes so `tag = 1` matches a single row, while the scan upper bound is still 1000000 (so the
-- earlier `cache <= upper_bound` guard does not catch it). If that cached 1000000 anchored the
-- swap, the residual-filtered 1000-row left input would be moved onto the build side over a right
-- side that now emits 1 row. Since only an Exact count anchors (a cached size is a heuristic
-- estimate), the tiny right side stays the build.
DROP TABLE IF EXISTS sl;
DROP TABLE IF EXISTS sr;

CREATE TABLE sl (k Int32, v Int32) ENGINE = MergeTree ORDER BY k;
INSERT INTO sl SELECT number, number FROM numbers(1000);

CREATE TABLE sr (k Int32, tag Int32) ENGINE = MergeTree ORDER BY k;
INSERT INTO sr SELECT number, 1 FROM numbers(1000000);   -- tag=1 matches all -> build 1000000, cache=1000000

-- Prime the cache with the 1000000-row build (sr is the default right side; both are upper bounds).
SELECT * FROM sl JOIN (SELECT k FROM sr WHERE tag = 1) AS rf ON sl.k = rf.k WHERE sl.v != -1
SETTINGS query_plan_join_swap_table = 'auto' FORMAT Null;

-- Data changes: `tag = 1` now matches one row; the table is still 1000000 rows (scan ub unchanged).
TRUNCATE TABLE sr;
INSERT INTO sr SELECT number, if(number = 0, 1, 2) FROM numbers(1000000);

SELECT * FROM sl JOIN (SELECT k FROM sr WHERE tag = 1) AS rf ON sl.k = rf.k WHERE sl.v != -1
SETTINGS query_plan_join_swap_table = 'auto', log_comment = '04357_join_choose_build_table_cached_not_anchor' FORMAT Null;

SYSTEM FLUSH LOGS query_log;

-- The right side now emits 1 row and must stay the build side; the 1000-row left input must not be
-- swapped onto it on the strength of the stale 1000000 cache.
SELECT
    if(ProfileEvents['JoinBuildTableRowCount'] < 100, 'ok', format('fail({}): build={}', query_id, ProfileEvents['JoinBuildTableRowCount'])),
    if(ProfileEvents['JoinResultRowCount'] = 1, 'ok', format('fail({}): result={}', query_id, ProfileEvents['JoinResultRowCount']))
FROM system.query_log
WHERE type = 'QueryFinish' AND event_date >= yesterday() AND event_time >= now() - 600
  AND query_kind = 'Select' AND current_database = currentDatabase()
  AND log_comment = '04357_join_choose_build_table_cached_not_anchor'
ORDER BY event_time DESC
LIMIT 1;

-- A stale-high cache must NOT raise the estimate above the current known bound. The cache is
-- max-like: it shrinks only when a new build drops below half the stored value, so after the table
-- shrinks the cache can stay above the current size indefinitely. `sc_r` is first built with 100000
-- rows (cached), then truncated to 600. On the next run its scan upper bound is 600, but the cache
-- still says 100000. If that stale value were used, `sc_l`'s upper bound (5000) would be below it
-- and `sc_l` (the larger input) would be swapped onto the build side. With the stale cache ignored
-- (kept out because it exceeds the current upper bound), the 600-row `sc_r` stays the build side.
DROP TABLE IF EXISTS sc_l;
DROP TABLE IF EXISTS sc_r;

CREATE TABLE sc_l (k Int32, v Int32) ENGINE = MergeTree ORDER BY k;
INSERT INTO sc_l SELECT number, number FROM numbers(5000);

CREATE TABLE sc_r (k Int32, w Int32) ENGINE = MergeTree ORDER BY k;
INSERT INTO sc_r SELECT number, number FROM numbers(100000);

-- Prime the cache with the large historical size (sc_r is the default build side; both inputs are
-- upper bounds, so no swap).
SELECT * FROM sc_l JOIN sc_r ON sc_l.k = sc_r.k WHERE sc_l.v != -1 AND sc_r.w != -1
SETTINGS query_plan_join_swap_table = 'auto' FORMAT Null;

-- Shrink sc_r far below the cached value (600 > 100000 / 2 is false, so the cache stays at 100000).
TRUNCATE TABLE sc_r;
INSERT INTO sc_r SELECT number, number FROM numbers(600);

SELECT * FROM sc_l JOIN sc_r ON sc_l.k = sc_r.k WHERE sc_l.v != -1 AND sc_r.w != -1
SETTINGS query_plan_join_swap_table = 'auto', log_comment = '04357_join_choose_build_table_stale_cache' FORMAT Null;

SYSTEM FLUSH LOGS query_log;

-- The current 600-row `sc_r` must stay the build side; the 5000-row `sc_l` must not be swapped onto
-- it on the strength of the stale 100000 cache.
SELECT
    if(ProfileEvents['JoinBuildTableRowCount'] BETWEEN 1 AND 2000, 'ok', format('fail({}): build={}', query_id, ProfileEvents['JoinBuildTableRowCount'])),
    if(ProfileEvents['JoinProbeTableRowCount'] BETWEEN 4000 AND 6000, 'ok', format('fail({}): probe={}', query_id, ProfileEvents['JoinProbeTableRowCount']))
FROM system.query_log
WHERE type = 'QueryFinish' AND event_date >= yesterday() AND event_time >= now() - 600
  AND query_kind = 'Select' AND current_database = currentDatabase()
  AND log_comment = '04357_join_choose_build_table_stale_cache'
ORDER BY event_time DESC
LIMIT 1;

DROP TABLE IF EXISTS f;
DROP TABLE IF EXISTS u;
DROP TABLE IF EXISTS sl;
DROP TABLE IF EXISTS sr;
DROP TABLE IF EXISTS sc_l;
DROP TABLE IF EXISTS sc_r;
