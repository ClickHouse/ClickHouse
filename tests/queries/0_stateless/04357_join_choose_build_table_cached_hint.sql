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

-- The cached size may also anchor the upper-bound-driven swap on the OPPOSITE side: a residual-
-- filtered left input (only an upper bound) can be swapped onto the build side when the right
-- input's size is known from the cache. `pf` is filtered to an upper bound of 1000; `cf` is
-- residual-filtered too (so without the cache it is only an upper bound), but a first build
-- records its 1000000-row size in the cache. With that cached size on the right, `pf`'s upper
-- bound (1000) is below it, so `pf` is swapped onto the build side. (A purely-derived estimate on
-- the right would NOT anchor this -- see the aggregation case in 04337.)
DROP TABLE IF EXISTS pf;
DROP TABLE IF EXISTS cf;

CREATE TABLE pf (k Int32, v Int32) ENGINE = MergeTree ORDER BY k;
INSERT INTO pf SELECT number, number FROM numbers(1000);

CREATE TABLE cf (k Int32, w Int32) ENGINE = MergeTree ORDER BY k;
INSERT INTO cf SELECT number, number FROM numbers(1000000);

-- Prime the cache: build `cf` (the default right side; both inputs are upper bounds, so no swap).
SELECT * FROM pf JOIN cf ON pf.k = cf.k WHERE pf.v != -1 AND cf.w != -1
SETTINGS query_plan_join_swap_table = 'auto' FORMAT Null;

-- Now `cf` is Cached; `pf`'s upper bound (1000) < cached `cf` (1000000), so `pf` is swapped to build.
SELECT * FROM pf JOIN cf ON pf.k = cf.k WHERE pf.v != -1 AND cf.w != -1
SETTINGS query_plan_join_swap_table = 'auto', log_comment = '04357_join_choose_build_table_cached_anchor' FORMAT Null;

SYSTEM FLUSH LOGS query_log;

SELECT
    if(ProfileEvents['JoinBuildTableRowCount'] BETWEEN 900 AND 1100, 'ok', format('fail({}): build={}', query_id, ProfileEvents['JoinBuildTableRowCount'])),
    if(ProfileEvents['JoinProbeTableRowCount'] BETWEEN 900000 AND 1100000, 'ok', format('fail({}): probe={}', query_id, ProfileEvents['JoinProbeTableRowCount']))
FROM system.query_log
WHERE type = 'QueryFinish' AND event_date >= yesterday() AND event_time >= now() - 600
  AND query_kind = 'Select' AND current_database = currentDatabase()
  AND log_comment = '04357_join_choose_build_table_cached_anchor'
ORDER BY event_time DESC
LIMIT 1;

DROP TABLE IF EXISTS f;
DROP TABLE IF EXISTS u;
DROP TABLE IF EXISTS pf;
DROP TABLE IF EXISTS cf;
