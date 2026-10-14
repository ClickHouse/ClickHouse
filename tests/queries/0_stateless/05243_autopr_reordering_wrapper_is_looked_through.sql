-- An expression that only reorders its columns is a wrapper the boundary search has to look through,
-- and whether the columns share a type has nothing to do with it. `SELECT b, a` over `(a UInt64, b
-- String)` is the case that says so: the planner emits the reorder as a step of its own - a `Project
-- names`, a `Projection` or the merged expression above a JOIN - and the columns that leave it are the
-- columns that came in, so the replicas ship the same bytes either side of it.
--
-- Deciding that from the two headers read off by position calls this a change of layout and refuses to
-- look through the step. What follows depends on where the step sits: above a JOIN, the boundary stops on
-- the reorder instead of the JOIN, its key matches nothing in the single-node plan, and the query records
-- no statistics at all - which is what this test would have caught.
--
-- The `Union` branch that reads from the other replicas is the other half of the same question: it is
-- never instrumented, only recognised, so a wrapper above it must not have to pass the same test. One
-- that did not would be mistaken for a second branch to instrument and the query skipped outright.

DROP TABLE IF EXISTS t_autopr_reorder_left;
DROP TABLE IF EXISTS t_autopr_reorder_right;

CREATE TABLE t_autopr_reorder_left(a UInt64, b String) ENGINE = MergeTree ORDER BY a;
CREATE TABLE t_autopr_reorder_right(a UInt64, b String) ENGINE = MergeTree ORDER BY a;

INSERT INTO t_autopr_reorder_left SELECT number, toString(number) FROM numbers(1e6);
INSERT INTO t_autopr_reorder_right SELECT number, toString(number) FROM numbers(1e5);

SET enable_parallel_replicas=1, automatic_parallel_replicas_mode=2, parallel_replicas_local_plan=1,
    parallel_replicas_for_non_replicated_merge_tree=1, max_parallel_replicas=3,
    cluster_for_parallel_replicas='parallel_replicas';
-- For runs with the old analyzer
SET enable_analyzer=1;
SET max_threads=4;
SET automatic_parallel_replicas_min_bytes_per_replica=0;
-- A randomized join order changes which side the reorder ends up above, and the shape is the point here.
SET query_plan_optimize_join_order_randomize=0;
-- Keep the reorder a step of its own rather than merged into the read below it.
SET query_plan_merge_expressions = 0;

-- Reordering two columns of unlike types, straight over a read.
SELECT b, a FROM (SELECT a, b FROM t_autopr_reorder_left)
FORMAT Null SETTINGS log_comment='05243_wrapper_over_read';

SELECT b, a FROM (SELECT a, b FROM t_autopr_reorder_left)
FORMAT Null SETTINGS parallel_replicas_plan_based=1, log_comment='05243_wrapper_over_read_plan_based';

-- The same reorder above a JOIN, where refusing to look through it leaves the boundary on a step the
-- single-node plan has no counterpart for and the query records nothing.
SELECT r.b, l.a FROM t_autopr_reorder_left AS l JOIN t_autopr_reorder_right AS r ON l.a = r.a
FORMAT Null SETTINGS log_comment='05243_wrapper_over_join';

SELECT r.b, l.a FROM t_autopr_reorder_left AS l JOIN t_autopr_reorder_right AS r ON l.a = r.a
FORMAT Null SETTINGS parallel_replicas_plan_based=1, log_comment='05243_wrapper_over_join_plan_based';

SET query_plan_merge_expressions = 1;

SET enable_parallel_replicas=0, automatic_parallel_replicas_mode=0;

SYSTEM FLUSH LOGS query_log;

-- Both figures non-zero means the boundary landed on a node that records what the replicas would send.
-- Zero against non-zero is all this asserts: the recorded bytes themselves depend on the codec.
SELECT log_comment,
    (ProfileEvents['RuntimeDataflowStatisticsInputBytes'] > 0)
        AND (ProfileEvents['RuntimeDataflowStatisticsOutputBytes'] > 0) AS statistics_collected
FROM system.query_log
WHERE (event_date >= yesterday()) AND (event_time >= NOW() - INTERVAL '15 MINUTES')
  AND (current_database = currentDatabase()) AND startsWith(log_comment, '05243_wrapper_over_') AND (type = 'QueryFinish')
ORDER BY log_comment
FORMAT TSVWithNames;

DROP TABLE t_autopr_reorder_left;
DROP TABLE t_autopr_reorder_right;
