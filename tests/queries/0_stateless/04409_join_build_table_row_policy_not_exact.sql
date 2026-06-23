-- Tags: no-parallel-replicas

-- A row-level security policy is applied while reading, after the mark ranges are chosen, so
-- `selected_rows` over-counts a protected table and must not be reported as an exact count: the
-- build-side choice could otherwise trust the over-count as a lower bound and swap a larger input
-- onto the build side. `estimateReadRowsCount` therefore classifies a read with a row policy as an
-- upper bound. (It checks `getRowLevelFilter()` directly, not only the source filter DAG, because
-- on the parallel-replicas plan the policy is not folded into that DAG; that distributed path
-- cannot be reproduced in a single-node stateless test, but this pins the build-side behavior.)
--
-- `rp` has 1000000 rows; the policy on the non-key column `secret` keeps 1, but `selected_rows`
-- stays ~1000000. The residual-filtered `lf` (upper bound 500) must NOT be swapped onto the build
-- side over the policy-protected `rp`, which yields a single row and stays the build side.

SET enable_analyzer = 1;
SET query_plan_join_swap_table = 'auto';
SET use_statistics = 0;
SET query_plan_optimize_join_order_limit = 10;
SET query_plan_optimize_join_order_randomize = 0;
SET collect_hash_table_stats_during_joins = 0;
SET use_hash_table_stats_for_join_reordering = 0;
SET enable_join_transitive_predicates = 1;
SET enable_join_runtime_filters = 0;

DROP ROW POLICY IF EXISTS rp_pol_04409 ON rp;
DROP TABLE IF EXISTS rp;
DROP TABLE IF EXISTS lf;

CREATE TABLE rp (k Int64, secret Int32) ENGINE = MergeTree ORDER BY k;
INSERT INTO rp SELECT number, number FROM numbers(1000000);
-- Non-key predicate -> the whole table is scanned (selected_rows ~1000000), policy keeps 1 row.
CREATE ROW POLICY rp_pol_04409 ON rp USING secret = 0 TO ALL;

CREATE TABLE lf (k Int64, v Int32) ENGINE = MergeTree ORDER BY k;
INSERT INTO lf SELECT number, number FROM numbers(500);

SELECT * FROM lf JOIN rp ON lf.k = rp.k WHERE lf.v != -1
SETTINGS log_comment = '04409_join_build_table_row_policy_not_exact' FORMAT Null;

SYSTEM FLUSH LOGS query_log;

-- The policy-protected side (1 row) must stay the build side; the 500-row left input must not be
-- swapped onto it on the strength of the 1000000-row pre-policy over-count.
SELECT
    if(ProfileEvents['JoinBuildTableRowCount'] BETWEEN 1 AND 100, 'ok', format('fail({}): build={}', query_id, ProfileEvents['JoinBuildTableRowCount'])),
    if(ProfileEvents['JoinProbeTableRowCount'] BETWEEN 400 AND 600, 'ok', format('fail({}): probe={}', query_id, ProfileEvents['JoinProbeTableRowCount'])),
    if(ProfileEvents['JoinResultRowCount'] = 1, 'ok', format('fail({}): result={}', query_id, ProfileEvents['JoinResultRowCount']))
FROM system.query_log
WHERE type = 'QueryFinish' AND event_date >= yesterday() AND event_time >= now() - 600
  AND query_kind = 'Select' AND current_database = currentDatabase()
  AND log_comment = '04409_join_build_table_row_policy_not_exact'
ORDER BY event_time DESC
LIMIT 1;

DROP ROW POLICY IF EXISTS rp_pol_04409 ON rp;
DROP TABLE IF EXISTS rp;
DROP TABLE IF EXISTS lf;
