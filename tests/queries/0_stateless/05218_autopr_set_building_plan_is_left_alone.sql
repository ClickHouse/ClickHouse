-- A plan that builds a set is rooted in the `CreatingSetStep` that fills it, and `addPlansForSets`
-- optimizes it like any other plan, so AutoPR gets to consider it. It must not: switching to the
-- parallel-replicas candidate replaces the root of the plan, and that root is what makes the plan a
-- set-building one - the candidate is built without it, so `CreatingSetsStep` would be handed a plan
-- carrying the subquery's own columns instead of the empty header it requires.
--
-- A `GLOBAL IN` runs its subquery as a plan of its own, which is what brings a set-building plan in front
-- of AutoPR here, and the optimization logs the refusal when it declines to take it. That log line is what
-- this test looks for, in both implementations of parallel replicas.

DROP TABLE IF EXISTS t_autopr_set_root_hits;
DROP TABLE IF EXISTS t_autopr_set_root_keys;

CREATE TABLE t_autopr_set_root_hits(WatchID UInt64, CounterID UInt32, UserID UInt64, URL String)
ENGINE = MergeTree ORDER BY (CounterID, UserID);
CREATE TABLE t_autopr_set_root_keys(a UInt64) ENGINE = MergeTree ORDER BY a;

INSERT INTO t_autopr_set_root_hits SELECT number, number % 100000, number * 7, repeat('u', 20) FROM numbers(1e6);
INSERT INTO t_autopr_set_root_keys SELECT number FROM numbers_mt(1e5);

SET enable_parallel_replicas=1, automatic_parallel_replicas_mode=1, parallel_replicas_local_plan=1,
    parallel_replicas_index_analysis_only_on_coordinator=1, parallel_replicas_for_non_replicated_merge_tree=1,
    max_parallel_replicas=3, cluster_for_parallel_replicas='parallel_replicas';
-- For runs with the old analyzer
SET enable_analyzer=1;
SET max_threads=4;
-- Don't let the min-bytes gate reject the reads before AutoPR looks at them, and keep every round of
-- index analysis visible rather than served from the condition cache.
SET automatic_parallel_replicas_min_bytes_per_replica=0, use_query_condition_cache=0;

-- Warm the statistics cache: the run that follows is the one that gets as far as pricing the switch.
SELECT sum(length(URL)) FROM t_autopr_set_root_hits
WHERE WatchID GLOBAL IN (SELECT a % 1000000 FROM t_autopr_set_root_keys) FORMAT Null;

SELECT sum(length(URL)) FROM t_autopr_set_root_hits
WHERE WatchID GLOBAL IN (SELECT a % 1000000 FROM t_autopr_set_root_keys)
FORMAT Null SETTINGS log_comment='05218_autopr_set_root_global_in';

-- And again under the plan-based implementation, which is where the refusal matters most: it is the one
-- that finds the boundary for this shape - the query-based walk gives up with "Cannot find step with
-- matching hash in single-node plan" - so it is also the one that goes on to consider the set-building
-- plan. Its statistics key is its own, hence its own warm-up run.
--
SELECT sum(length(URL)) FROM t_autopr_set_root_hits
WHERE WatchID GLOBAL IN (SELECT a % 1000000 FROM t_autopr_set_root_keys)
FORMAT Null SETTINGS parallel_replicas_plan_based=1;

SELECT sum(length(URL)) FROM t_autopr_set_root_hits
WHERE WatchID GLOBAL IN (SELECT a % 1000000 FROM t_autopr_set_root_keys)
FORMAT Null SETTINGS parallel_replicas_plan_based=1, log_comment='05218_autopr_set_root_global_in_plan_based';

SET enable_parallel_replicas=0, automatic_parallel_replicas_mode=0;

SYSTEM FLUSH LOGS query_log, text_log;

-- Ask the optimization itself. It says so when it refuses, and the refusal is what is being asserted, so
-- the message is the closest thing to the behaviour there is. Counting instead - rounds of index
-- analysis, or `ParallelReplicasQueryCount` - measures what the refusal happens to save, and both of
-- those move with the environment: the rounds depend on what index analysis can reuse, and the query
-- count is raised when the coordinator is destroyed, which need not happen on a thread the query is
-- still accounted to.
WITH refused AS
(
    SELECT DISTINCT query_id
    FROM system.text_log
    WHERE (event_date >= yesterday()) AND (logger_name = 'optimizeTree')
      AND (message LIKE '%The plan builds a set, its root must be preserved%')
)
SELECT log_comment, query_id IN (SELECT query_id FROM refused) AS set_plan_left_alone
FROM system.query_log
WHERE (event_date >= yesterday()) AND (event_time >= NOW() - INTERVAL '15 MINUTES')
  AND (current_database = currentDatabase()) AND startsWith(log_comment, '05218_autopr_set_root_global_in')
  AND (type = 'QueryFinish')
ORDER BY log_comment
FORMAT TSVWithNames;

DROP TABLE t_autopr_set_root_hits;
DROP TABLE t_autopr_set_root_keys;
