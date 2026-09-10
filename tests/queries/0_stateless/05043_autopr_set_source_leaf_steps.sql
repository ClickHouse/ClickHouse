-- The plan simplicity check for automatic parallel replicas looks only at the query's own plan. The
-- plan that builds an `IN` set is not part of it, so a step in there that cannot collect dataflow
-- statistics - `ReadFromSystemNumbers` for a set over `numbers`, a `UnionStep` for a set over a
-- union, a prepared source for a dictionary or a system table - must not disqualify the query.
-- Widening the check to walk set source plans would do exactly that, which is why it does not.
--
-- Such a walk would also find nothing to reject: by the time the check runs, the set no longer holds
-- its source plan. That was measured across both values of `use_index_for_in_with_subqueries`, with
-- the `IN` on an indexed and on a plain column, and over a `Merge` table - the source is gone in
-- every one.
--
-- `RuntimeDataflowStatisticsInputBytes` is non-zero only for a query the check let through and the
-- optimization then instrumented, so it is what says this query is still a candidate.

DROP TABLE IF EXISTS t_autopr_set_source;

CREATE TABLE t_autopr_set_source (key UInt64, non_key UInt64, pad String) ENGINE = MergeTree ORDER BY key;
INSERT INTO t_autopr_set_source SELECT number, number % 5000, repeat('x', 20) FROM numbers(200000);

SET enable_parallel_replicas = 1, automatic_parallel_replicas_mode = 1, parallel_replicas_local_plan = 1,
    parallel_replicas_for_non_replicated_merge_tree = 1, max_parallel_replicas = 3,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';
SET enable_analyzer = 1;

-- The `IN` is on a column outside the primary key, so the set is not there to be turned into a key
-- condition - it is only ever a filter, which is the shape that leaves the set source plan otherwise
-- uninteresting to the optimization.
SELECT key, pad FROM t_autopr_set_source WHERE non_key IN (SELECT number FROM numbers(1000))
FORMAT Null SETTINGS log_comment = '05043_autopr_set_source_leaf_steps_1_leaf';

-- A union inside the set is the same question one step further: `UnionStep` does not collect dataflow
-- statistics either, and a check that walked the set source plan would reject this too.
SELECT key, pad FROM t_autopr_set_source
WHERE non_key IN (SELECT number FROM numbers(500) UNION ALL SELECT number FROM numbers(500, 500))
FORMAT Null SETTINGS log_comment = '05043_autopr_set_source_leaf_steps_2_union';

SET enable_parallel_replicas = 0, automatic_parallel_replicas_mode = 0;

SYSTEM FLUSH LOGS query_log;

SELECT log_comment, ProfileEvents['RuntimeDataflowStatisticsInputBytes'] > 0 AS still_a_candidate
FROM system.query_log
WHERE (event_date >= yesterday()) AND (event_time >= (NOW() - toIntervalMinute(15)))
    AND (current_database = currentDatabase())
    AND (log_comment IN ('05043_autopr_set_source_leaf_steps_1_leaf', '05043_autopr_set_source_leaf_steps_2_union'))
    AND (type = 'QueryFinish') AND is_initial_query
ORDER BY log_comment
FORMAT TSVWithNames;

DROP TABLE t_autopr_set_source;
