-- Regression test: a stateful function (e.g. `logTrace`, `neighbor`) in the SELECT list of an
-- otherwise trivial `LIMIT` query requires a single deterministic input stream, so parallel replicas
-- must be disabled for it -- EVEN when a hidden reader-side filter (here a row policy) is present.
--
-- The hidden-filter check used to be conflated with the source-cap decision: as soon as a row policy
-- (or `additional_table_filters`) applied, `maxBlockSizeByLimit` / `mainQueryNodeBlockSizeByLimit`
-- short-circuited before setting the stateful flag, so `adjustParallelReplicasAfterAnalysis`
-- (old analyzer) / the planner branch never disabled parallel replicas. Suppressing the source cap
-- for a hidden filter is correct (the filter can drop rows before the LIMIT), but the separate
-- single-deterministic-stream requirement for a stateful projection must still fire. This is a
-- companion to `04551_parallel_replicas_stateful_limit` (same query without a row policy).

DROP ROW POLICY IF EXISTS rp_04558 ON t_pr_rp_stateful;
DROP TABLE IF EXISTS t_pr_rp_stateful;

CREATE TABLE t_pr_rp_stateful (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 8;
INSERT INTO t_pr_rp_stateful SELECT number, number * 10 FROM numbers(1000);

-- A row policy is a hidden reader-side filter: it does not appear in the query AST as a `WHERE`,
-- but it is collected into `query_info.filter_asts` and applied inside the reader.
CREATE ROW POLICY rp_04558 ON t_pr_rp_stateful FOR SELECT USING k >= 100 TO ALL;

SET automatic_parallel_replicas_mode = 0;
SET parallel_replicas_only_with_analyzer = 0;  -- necessary for CI run with disabled analyzer
SET enable_parallel_replicas = 2, max_parallel_replicas = 3, parallel_replicas_for_non_replicated_merge_tree = 1,
    parallel_replicas_min_number_of_rows_per_replica = 0,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';

-- Sanity: a plain query under the same row policy DOES use parallel replicas in this setup.
SELECT count(), sum(k) FROM t_pr_rp_stateful SETTINGS log_comment = '04558_plain', enable_analyzer = 1 FORMAT Null;

-- A stateful function in the select list of a trivial-`LIMIT` query under a row policy must NOT use
-- parallel replicas (on both analyzers).
SELECT ignore(logTrace('04558')), k FROM t_pr_rp_stateful LIMIT 1 SETTINGS log_comment = '04558_stateful_new', enable_analyzer = 1 FORMAT Null;
SELECT ignore(logTrace('04558')), k FROM t_pr_rp_stateful LIMIT 1 SETTINGS log_comment = '04558_stateful_old', enable_analyzer = 0 FORMAT Null;

SYSTEM FLUSH LOGS query_log;

SELECT 'plain', ProfileEvents['ParallelReplicasUsedCount'] > 0
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600 AND type = 'QueryFinish'
  AND query_id IN (SELECT query_id FROM system.query_log WHERE current_database = currentDatabase()
    AND log_comment = '04558_plain' AND type = 'QueryFinish' AND initial_query_id = query_id)
SETTINGS enable_parallel_replicas = 0;

SELECT 'stateful_new', ProfileEvents['ParallelReplicasUsedCount'] > 0
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600 AND type = 'QueryFinish'
  AND query_id IN (SELECT query_id FROM system.query_log WHERE current_database = currentDatabase()
    AND log_comment = '04558_stateful_new' AND type = 'QueryFinish' AND initial_query_id = query_id)
SETTINGS enable_parallel_replicas = 0;

SELECT 'stateful_old', ProfileEvents['ParallelReplicasUsedCount'] > 0
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600 AND type = 'QueryFinish'
  AND query_id IN (SELECT query_id FROM system.query_log WHERE current_database = currentDatabase()
    AND log_comment = '04558_stateful_old' AND type = 'QueryFinish' AND initial_query_id = query_id)
SETTINGS enable_parallel_replicas = 0;

DROP ROW POLICY rp_04558 ON t_pr_rp_stateful;
DROP TABLE t_pr_rp_stateful;
