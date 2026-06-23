-- Tags: no-parallel-replicas

-- The upper-bound build-side swap must work under the DPhyp join-order algorithm too. DPhyp builds
-- its own leaf `DPJoinEntry` nodes; if those leaves do not carry the row-count kind, every base
-- relation looks `Unknown` to `chooseJoinOrder` and the swap (which needs an `UpperBound` left and
-- an `Exact` right) silently never fires. This pins that the kind is threaded through DPhyp.
--
-- `sd` (1000 rows, residual-filtered on a non-key column -> upper bound 1000) joins `bd` (1000000
-- rows, unfiltered -> exact). The filtered small side must be swapped onto the build side; if the
-- DPhyp leaves were `Unknown`, `bd` (1000000) would stay the build side instead.

SET enable_analyzer = 1;
SET query_plan_join_swap_table = 'auto';
SET use_statistics = 0;
SET query_plan_optimize_join_order_limit = 10;
SET query_plan_optimize_join_order_randomize = 0;
SET collect_hash_table_stats_during_joins = 0;
SET use_hash_table_stats_for_join_reordering = 0;
SET enable_join_transitive_predicates = 1;
SET enable_join_runtime_filters = 0;
-- Force the DPhyp algorithm specifically (this is the point of the test).
SET query_plan_optimize_join_order_algorithm = 'dphyp';

DROP TABLE IF EXISTS sd;
DROP TABLE IF EXISTS bd;

CREATE TABLE sd (k Int32, v Int32) ENGINE = MergeTree ORDER BY k;
INSERT INTO sd SELECT number, number FROM numbers(1000);

CREATE TABLE bd (k Int32) ENGINE = MergeTree ORDER BY k;
INSERT INTO bd SELECT number FROM numbers(1000000);

-- `sd.v != -1` keeps all 1000 rows but is not index-usable -> sd is an upper bound (1000); bd is
-- exact (1000000). upperBound(1000) < exact(1000000) -> sd is swapped onto the build side.
SELECT * FROM (SELECT k FROM sd WHERE v != -1) AS s JOIN bd ON s.k = bd.k
SETTINGS log_comment = '04408_join_choose_build_table_upper_bound_dphyp' FORMAT Null;

SYSTEM FLUSH LOGS query_log;

-- The filtered `sd` (1000) must be the build side; `bd` (1000000) the probe.
SELECT
    if(ProfileEvents['JoinBuildTableRowCount'] BETWEEN 900 AND 1100, 'ok', format('fail({}): build={}', query_id, ProfileEvents['JoinBuildTableRowCount'])),
    if(ProfileEvents['JoinProbeTableRowCount'] BETWEEN 900000 AND 1100000, 'ok', format('fail({}): probe={}', query_id, ProfileEvents['JoinProbeTableRowCount']))
FROM system.query_log
WHERE type = 'QueryFinish' AND event_date >= yesterday() AND event_time >= now() - 600
  AND query_kind = 'Select' AND current_database = currentDatabase()
  AND log_comment = '04408_join_choose_build_table_upper_bound_dphyp'
ORDER BY event_time DESC
LIMIT 1;

DROP TABLE IF EXISTS sd;
DROP TABLE IF EXISTS bd;
