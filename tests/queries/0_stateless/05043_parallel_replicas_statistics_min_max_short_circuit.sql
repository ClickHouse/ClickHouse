-- Tags: no-parallel
-- - no-parallel - due to usage of fail points

-- Regression test for the statistics-based min/max/count short-circuit under parallel replicas
-- (the `_statistics_min_max_projection` counterpart of
-- 04545_parallel_replicas_projection_short_circuit_unknown_stream). When the statistics block
-- covers all parts, the `ReadFromMergeTree` step is fully replaced by a prepared source on the
-- initiator's local plan, so it must send the empty-ranges announcement to the coordinator.
-- Without it, a follower that does not take the same short-circuit requests a stream the
-- coordinator never registered, tripping "Got read request from replica N for unknown stream ..."
-- (exception with `LOGICAL_ERROR`). The failpoint forces a follower to skip the short-circuit,
-- deterministically reproducing the plan divergence a homogeneous single-server cluster cannot
-- otherwise create.

DROP TABLE IF EXISTS t_pr_stats_short_circuit;
CREATE TABLE t_pr_stats_short_circuit
(
    id UInt64,
    v Int32
)
ENGINE = MergeTree ORDER BY id
SETTINGS auto_statistics_types = 'minmax', index_granularity = 32;

SET materialize_statistics_on_insert = 1;
INSERT INTO t_pr_stats_short_circuit SELECT number, toInt32(number % 1000) - 500 FROM numbers(5000);

SET automatic_parallel_replicas_mode = 0;
SET enable_parallel_replicas = 1, max_parallel_replicas = 3, parallel_replicas_for_non_replicated_merge_tree = 1;
SET cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';
SET parallel_replicas_local_plan = 1;
-- Pin every setting the short-circuit eligibility depends on (see the comments in test 04545).
SET enable_analyzer = 1;
SET optimize_use_projections = 1, optimize_use_implicit_projections = 1;
SET optimize_aggregation_in_order = 0, force_aggregation_in_order = 0;
SET aggregate_functions_null_for_empty = 0;
SET use_statistics_for_min_max_aggregation = 1;

-- Liveness: the query below must actually take the statistics short-circuit on the initiator,
-- otherwise the regression is not being exercised. Assert the pseudo projection is selected (prints 1).
SELECT 'statistics_projection_used', count() > 0 FROM (EXPLAIN SELECT min(v), max(v), count() FROM t_pr_stats_short_circuit) WHERE explain ILIKE '%_statistics_min_max_projection%';

SYSTEM ENABLE FAILPOINT parallel_replicas_skip_aggregate_projection_on_follower;

-- All parts have materialized statistics, so the prepared source consumes the whole read and no
-- parent parts remain. Before the fix this tripped the "unknown stream" exception on the coordinator.
SELECT min(v), max(v), count() FROM t_pr_stats_short_circuit SETTINGS log_comment = '05043_stats_minmax';

SYSTEM DISABLE FAILPOINT parallel_replicas_skip_aggregate_projection_on_follower;

-- Liveness: prove parallel replicas actually engaged and a follower reached the coordinator's
-- request path (where the bug lives). Prints 1.
SYSTEM FLUSH LOGS query_log;
SELECT
    log_comment,
    ProfileEvents['ParallelReplicasHandleRequestMicroseconds'] > 0
FROM system.query_log
WHERE current_database = currentDatabase()
  AND log_comment = '05043_stats_minmax'
  AND type = 'QueryFinish'
  AND query_id = initial_query_id
  AND event_time >= now() - INTERVAL 600 SECOND
ORDER BY log_comment;

DROP TABLE t_pr_stats_short_circuit;
