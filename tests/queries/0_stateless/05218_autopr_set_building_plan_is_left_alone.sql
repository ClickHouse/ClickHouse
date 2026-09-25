-- A plan that builds a set is rooted in the `CreatingSetStep` that fills it, and `addPlansForSets`
-- optimizes it like any other plan, so AutoPR gets to consider it. It must not: switching to the
-- parallel-replicas candidate replaces the root of the plan, and that root is what makes the plan a
-- set-building one - the candidate is built without it, so `CreatingSetsStep` would be handed a plan
-- carrying the subquery's own columns instead of the empty header it requires.
--
-- Considering such a plan is not free even when nothing comes of it: AutoPR builds a probe plan to price
-- the switch, and building one runs index analysis. A `GLOBAL IN` runs its subquery as a plan of its own,
-- which makes both counts easy to read - how many queries AutoPR weighed up, and how many rounds of index
-- analysis that took.
--
-- Counts, not wall time, so they are the same under sanitizers.

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
-- The number to watch here is the consideration count, not the rounds: having matched, this
-- implementation reaches its verdict from the statistics instead of re-analysing, so the refusal costs it
-- no round at all and only the count moves.
SELECT sum(length(URL)) FROM t_autopr_set_root_hits
WHERE WatchID GLOBAL IN (SELECT a % 1000000 FROM t_autopr_set_root_keys)
FORMAT Null SETTINGS parallel_replicas_plan_based=1;

SELECT sum(length(URL)) FROM t_autopr_set_root_hits
WHERE WatchID GLOBAL IN (SELECT a % 1000000 FROM t_autopr_set_root_keys)
FORMAT Null SETTINGS parallel_replicas_plan_based=1, log_comment='05218_autopr_set_root_global_in_plan_based';

SET enable_parallel_replicas=0, automatic_parallel_replicas_mode=0;

SYSTEM FLUSH LOGS query_log;

-- `ParallelReplicasQueryCount` counts the queries AutoPR weighed up, the discarded probes included, so
-- one per row is the query itself and a second would be the set-building plan behind its `GLOBAL IN`.
SELECT log_comment,
    ProfileEvents['ParallelReplicasQueryCount'] AS autopr_considered,
    ProfileEvents['IndexAnalysisRounds'] AS index_analysis_rounds
FROM system.query_log
WHERE (event_date >= yesterday()) AND (event_time >= NOW() - INTERVAL '15 MINUTES')
  AND (current_database = currentDatabase()) AND (log_comment LIKE '05218_autopr_set_root_global_in%') AND (type = 'QueryFinish')
ORDER BY log_comment
FORMAT TSVWithNames;

DROP TABLE t_autopr_set_root_hits;
DROP TABLE t_autopr_set_root_keys;
