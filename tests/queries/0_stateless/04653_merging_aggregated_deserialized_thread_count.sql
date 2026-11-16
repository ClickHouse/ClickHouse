-- A `MergingAggregatedStep` that the parallel replicas rewrite builds from a deserialized
-- `AggregatingStep` must not keep the sentinel thread count 0. The same query without a serialized
-- plan is the oracle, and two further oracles pin that a plan was really shipped and the rewrite ran.

DROP TABLE IF EXISTS t_merging_aggregated_threads;

CREATE TABLE t_merging_aggregated_threads (a UInt64, k UInt64)
ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 8;

INSERT INTO t_merging_aggregated_threads SELECT number, number % 7 FROM numbers(2000);
INSERT INTO t_merging_aggregated_threads SELECT number + 2000, number % 7 FROM numbers(2000);

-- The analyzer is load-bearing, not incidental: without it `canUseTaskBasedParallelReplicas`
-- returns false (`parallel_replicas_only_with_analyzer` defaults true), so the rewrite that
-- builds the unresolved step never runs and every assertion below would silently pass on a
-- different route. Pinned as a session `SET` so it also covers the observing queries and
-- defeats `compatibility` randomization.
SET enable_analyzer = 1;
SET allow_experimental_parallel_reading_from_replicas = 1;
SET automatic_parallel_replicas_mode = 0;
SET max_parallel_replicas = 3;
SET parallel_replicas_plan_based = 1;
SET parallel_replicas_for_non_replicated_merge_tree = 1;
SET cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';
-- The test verifies a plan packet on a same-version local cluster. Hedged connections retain
-- the SQL fallback for a future, unverified rolling-upgrade peer, so avoid that unrelated route.
SET use_hedged_requests = 0;

-- The parallel replicas optimizer rewrite builds a `MergingAggregatedStep` from the params of a
-- deserialized `AggregatingStep`, which carry the "resolve locally later" sentinel 0 as the thread
-- count. Unlike the exchange-adding distributed plan rewrites, this one adds no extra stage, so
-- the step is never re-serialized and the 0 survives to pipeline construction.
-- Both memory-efficient settings must be off, otherwise the aggregation asks for results in
-- bucket order and the resize width is the literal 1, which masks the stale 0.
SELECT k, sum(a) FROM cluster('test_cluster_one_shard_three_replicas_localhost', currentDatabase(), 't_merging_aggregated_threads')
GROUP BY k ORDER BY k
SETTINGS serialize_query_plan = 1,
         distributed_aggregation_memory_efficient = 0,
         enable_memory_bound_merging_of_aggregation_results = 0,
         -- This test asserts the serialized-plan path itself. Keep plan-level limits at their
         -- defaults: a hedged connection must deliberately use SQL instead when a future replica
         -- could be an older peer that cannot receive serialized execution limits.
         max_threads = 0,
         use_concurrency_control = 0,
         -- Required by the second firing oracle below. It defaults true, but it flipped false to
         -- true in the 24.3 block of `SettingsChangesHistory.cpp`, so a `compatibility` draw
         -- below 24.3 turns it off and that oracle would silently read 0.
         log_processors_profiles = 1,
         log_comment = '04653_merging_aggregated_deserialized_thread_count';

-- Same query without a serialized plan: no step is ever deserialized, so this is the oracle.
SELECT k, sum(a) FROM cluster('test_cluster_one_shard_three_replicas_localhost', currentDatabase(), 't_merging_aggregated_threads')
GROUP BY k ORDER BY k
SETTINGS serialize_query_plan = 0,
         distributed_aggregation_memory_efficient = 0,
         enable_memory_bound_merging_of_aggregation_results = 0;

-- Without a `GROUP BY` the merge step is still built by the same rewrite.
SELECT sum(a) FROM cluster('test_cluster_one_shard_three_replicas_localhost', currentDatabase(), 't_merging_aggregated_threads')
SETTINGS serialize_query_plan = 1,
         distributed_aggregation_memory_efficient = 0,
         enable_memory_bound_merging_of_aggregation_results = 0;

-- Firing oracle: assert the first query really did ship a serialized plan to a replica that then
-- read data from it. Without this, every assertion above is satisfied by an ordinary plan and a
-- future gating change would silently empty the test.
-- A secondary query carrying `serialize_query_plan = 1` is one that executed a plan deserialized
-- from the wire (it is absent from the secondaries of the `serialize_query_plan = 0` shape), and
-- `SelectedMarks > 0` pins that it reached the read rather than only being announced.
-- Take the newest matching row rather than aggregating over history: CI passes a fixed
-- `--database` in some jobs, so one database serves many executions and an aggregate could be
-- satisfied by an earlier run.
-- Secondary queries run as the `default` user, so their `current_database` is `default` rather
-- than the test database and they cannot be filtered by `current_database = currentDatabase()`
-- directly. Scope them through their initiator, which does run in `currentDatabase()`.
SYSTEM FLUSH LOGS query_log, processors_profile_log;

SELECT argMax(Settings['serialize_query_plan'] = '1' AND ProfileEvents['SelectedMarks'] > 0, event_time_microseconds)
FROM system.query_log
WHERE initial_query_id = (
        SELECT argMax(query_id, event_time_microseconds)
        FROM system.query_log
        WHERE current_database = currentDatabase()
          AND log_comment = '04653_merging_aggregated_deserialized_thread_count'
          AND type = 'QueryFinish'
          AND is_initial_query
          AND event_date >= yesterday() AND event_time >= now() - 600)
  AND type = 'QueryFinish'
  AND NOT is_initial_query
  AND event_date >= yesterday() AND event_time >= now() - 600
SETTINGS enable_parallel_replicas = 0;

-- Second firing oracle, pinning a different axis: the parallel replicas aggregation rewrite
-- fired. A `MergingAggregated` step on a secondary is attributable to it: of the four creators
-- of that step, the old-analyzer one is excluded by the `enable_analyzer` pin above, the planner
-- one runs on the initiator, and `makeDistributed` throws `SUPPORT_IS_DISABLED` when combined
-- with parallel replicas, leaving the rewrite that builds the step this fix repairs. Where the
-- rewrite declines, the secondary stops at partial aggregation (`setFinal(false)`), so it reports
-- `Aggregating` but never `MergingAggregated`.
-- The pairing with the oracle above is deliberate and neither cell may be dropped: this one does
-- NOT pin that the plan was deserialized (the `serialize_query_plan = 0` statement above also
-- produces a non-initial `MergingAggregated`), and that one does not pin that the rewrite fired.
-- `processors_profile_log` has no `log_comment` column, so resolve the initial query id from
-- `system.query_log` and keep the rows whose `query_id` differs from it. Newest initial query
-- only, for the fixed-`--database` reason above.
SELECT countIf(plan_step_name = 'MergingAggregated') > 0
FROM system.processors_profile_log
WHERE initial_query_id = (
        SELECT argMax(query_id, event_time_microseconds)
        FROM system.query_log
        WHERE current_database = currentDatabase()
          AND log_comment = '04653_merging_aggregated_deserialized_thread_count'
          AND type = 'QueryFinish'
          AND is_initial_query
          AND event_date >= yesterday() AND event_time >= now() - 600)
  AND query_id != initial_query_id
  AND event_date >= yesterday() AND event_time >= now() - 600
SETTINGS enable_parallel_replicas = 0;

DROP TABLE t_merging_aggregated_threads;
