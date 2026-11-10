-- Tags: no-parallel
-- - no-parallel - due to usage of fail points

-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/110518
-- When a projection optimization consumes the whole read on the initiator's parallel-replicas local
-- plan, the ReadFromMergeTree step is replaced by a prepared source, so it never sends the empty-ranges
-- announcement to the coordinator. A follower that does not take the same short-circuit then requests a
-- stream the coordinator never registered, tripping "Got read request from replica N for unknown
-- stream ..." (LOGICAL_ERROR / server abort in debug and sanitizer builds). This affects four fully
-- short-circuiting paths: minmax-count and exact-count aggregate projections, a stored aggregate
-- projection selecting no ranges, and a stored normal projection selecting no ranges. The failpoint
-- forces a follower to skip the short-circuit, deterministically reproducing the plan divergence a
-- homogeneous single-server cluster cannot otherwise create.

DROP TABLE IF EXISTS t_pr_short_circuit;
CREATE TABLE t_pr_short_circuit
(
    id UInt64,
    v UInt64,
    region String,
    PROJECTION agg_proj (SELECT sum(v) GROUP BY region),
    PROJECTION normal_proj (SELECT id, region ORDER BY region)
)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 32;
-- Keep the parts unmerged so the exact-count predicate matches exactly one part boundary.
SYSTEM STOP MERGES t_pr_short_circuit;
INSERT INTO t_pr_short_circuit SELECT number, number, if(number % 2 = 0, 'a', 'b') FROM numbers(5000);
INSERT INTO t_pr_short_circuit VALUES (999999999, 0, 'a');

SET automatic_parallel_replicas_mode = 0;
SET enable_parallel_replicas = 1, max_parallel_replicas = 3, parallel_replicas_for_non_replicated_merge_tree = 1;
SET cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';
SET parallel_replicas_local_plan = 1;
-- Pin every setting the short-circuit eligibility depends on: the local plan and the projection
-- optimization both require the analyzer, aggregation-in-order or aggregate_functions_null_for_empty
-- disable the projection, and the runner randomizes optimize_aggregation_in_order. Without pinning them
-- a randomized run could take an ordinary scan and never exercise the bug.
SET enable_analyzer = 1;
SET optimize_use_projections = 1, optimize_use_implicit_projections = 1;
SET optimize_aggregation_in_order = 0, force_aggregation_in_order = 0;
SET aggregate_functions_null_for_empty = 0;

-- Liveness: the queries below must actually take the short-circuit on the initiator, otherwise the
-- regression is not being exercised. Assert each projection is selected (prints 1).
SELECT 'exact_count_used', count() > 0 FROM (EXPLAIN SELECT count() FROM t_pr_short_circuit WHERE id = 999999999) WHERE explain ILIKE '%_exact_count_projection%';
SELECT 'minmax_used', count() > 0 FROM (EXPLAIN SELECT min(id), max(id), count() FROM t_pr_short_circuit) WHERE explain ILIKE '%_minmax_count_projection%';
SELECT 'agg_proj_used', count() > 0 FROM (EXPLAIN SELECT sum(v) FROM t_pr_short_circuit WHERE region = 'nonexistent' GROUP BY region) WHERE explain ILIKE '%agg_proj%';
SELECT 'normal_proj_used', count() > 0 FROM (EXPLAIN PLAN SELECT id FROM t_pr_short_circuit WHERE region = 'nonexistent') WHERE explain ILIKE '%normal_proj%';

SYSTEM ENABLE FAILPOINT parallel_replicas_skip_aggregate_projection_on_follower;

-- Exact-count short-circuit: the predicate matches exactly one row that fills a whole part, so all parent
-- parts are consumed and the ReadFromMergeTree is fully replaced. Before the fix this aborts the server.
SELECT count() FROM t_pr_short_circuit WHERE id = 999999999 SETTINGS log_comment = '04545_exact_count';

-- Minmax-count short-circuit: min/max/count answered entirely from the minmax projection (always leaves
-- no parent parts). Same failure path as above.
SELECT min(id), max(id), count() FROM t_pr_short_circuit SETTINGS log_comment = '04545_minmax';

-- Stored aggregate projection with an always-false filter: the projection read selects no ranges, so it
-- is replaced by a prepared source and no parent parts remain.
SELECT sum(v) FROM t_pr_short_circuit WHERE region = 'nonexistent' GROUP BY region SETTINGS log_comment = '04545_agg_proj';

-- Stored normal projection with an always-false filter (Default mode, no ORDER BY): same short-circuit.
SELECT id FROM t_pr_short_circuit WHERE region = 'nonexistent' SETTINGS log_comment = '04545_normal_proj';

SYSTEM DISABLE FAILPOINT parallel_replicas_skip_aggregate_projection_on_follower;

-- Liveness: prove parallel replicas actually engaged and a follower reached the coordinator's request
-- path (where the bug lives). If parallel replicas silently did not engage, the failpoint would be a
-- no-op and the queries above would pass trivially; ParallelReplicasHandleRequestMicroseconds > 0
-- confirms the coordinator handled a follower read request. Prints 1 for every query.
SYSTEM FLUSH LOGS query_log;
SELECT
    log_comment,
    ProfileEvents['ParallelReplicasHandleRequestMicroseconds'] > 0
FROM system.query_log
WHERE current_database = currentDatabase()
  AND log_comment IN ('04545_exact_count', '04545_minmax', '04545_agg_proj', '04545_normal_proj')
  AND type = 'QueryFinish'
  AND query_id = initial_query_id
  AND event_time >= now() - INTERVAL 600 SECOND
ORDER BY log_comment;

DROP TABLE t_pr_short_circuit;
