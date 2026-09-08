-- The node the automatic parallel replicas optimization instruments has to be the one the replicas
-- would actually send from. `findTopNodeOfReplicasPlan` looks through the wrappers between the `Union`
-- and that node, and an `ExpressionStep` may only be looked through when what comes out of it is what
-- went in.
--
-- A projection is not that. The first-stage planner puts the query's projection list below the `Union`
-- as its own `ExpressionStep` (`Planner::buildQueryPlanIfNeeded`, and
-- `PlannerExpressionAnalysis::analyzeProjection` which builds the DAG), and that DAG is arbitrary. It
-- can keep the column count, the column positions and the type names while changing how wide the values
-- are, which a comparison of output headers cannot see. `repeat(a, 100)` over a `String` is exactly that
-- shape: the header stays `String, String` either side of the step, while the bytes the replicas send
-- grow several times over. Peeling it would instrument the rename below it and cost the query on a
-- fraction of what it really ships.

DROP TABLE IF EXISTS t_autopr_projection;

CREATE TABLE t_autopr_projection(a String, b String) ENGINE = MergeTree ORDER BY b;

SET enable_parallel_replicas=1, automatic_parallel_replicas_mode=2, parallel_replicas_local_plan=1,
    parallel_replicas_for_non_replicated_merge_tree=1, max_parallel_replicas=3,
    cluster_for_parallel_replicas='test_cluster_one_shard_three_replicas_localhost';

SET enable_analyzer=1;
SET max_threads=4;
SET automatic_parallel_replicas_min_bytes_per_replica=0;

INSERT INTO t_autopr_projection SELECT toString(number), toString(number) FROM numbers(1e6);

-- Expression merging would fold the projection together with the rename below it, leaving the merged
-- step directly on the reading step where the search stops regardless of what it thinks of the step. Pin
-- the merging off so the projection stays a step of its own and the choice is actually exercised.
SET query_plan_merge_expressions = 0;

-- The baseline: the projection here really is a rename, so the boundary is the rename above the read
-- either way and the recorded output is the two columns as they were read.
SELECT a, b FROM t_autopr_projection
FORMAT Null SETTINGS log_comment='05136_autopr_projection_rename';

-- The same shape with the projection widening one of the columns a hundredfold.
SELECT repeat(a, 100) AS a, b FROM t_autopr_projection
FORMAT Null SETTINGS log_comment='05136_autopr_projection_wide';

SET query_plan_merge_expressions = 1;

SET enable_parallel_replicas=0, automatic_parallel_replicas_mode=0;

SYSTEM FLUSH LOGS query_log;

-- Comparing the two recorded outputs rather than either against its input keeps the check independent of
-- the compression codec and the block sizing, which move both queries the same way. Instrumenting the
-- projection puts the widened column into the estimate and the wide query records several times the
-- baseline; peeling the projection instruments the same rename in both and the two come out level.
SELECT
    maxIf(ProfileEvents['RuntimeDataflowStatisticsOutputBytes'], log_comment = '05136_autopr_projection_wide')
        > (3 * maxIf(ProfileEvents['RuntimeDataflowStatisticsOutputBytes'], log_comment = '05136_autopr_projection_rename'))
        AS projection_output_counted
FROM system.query_log
WHERE (event_date >= yesterday()) AND (event_time >= (NOW() - toIntervalMinute(15))) AND (current_database = currentDatabase()) AND (log_comment LIKE '05136_autopr_projection_%') AND (type = 'QueryFinish')
FORMAT TSVWithNames;

DROP TABLE t_autopr_projection;
