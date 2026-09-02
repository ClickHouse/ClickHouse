-- The plan simplicity check for automatic parallel replicas walks the plans that build the `IN` sets
-- as well as the plan itself. Those set source plans are never instrumented for dataflow statistics,
-- so a step there that cannot collect them - `ReadFromSystemNumbers` for a set over `numbers`, a
-- prepared source for a dictionary or a system table - must not disqualify the query. Only a step
-- holding plans of its own may, because a set underneath one of those reaches neither the probe plan
-- nor the accepted one.
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
FORMAT Null SETTINGS log_comment = '05043_autopr_set_source_leaf_steps';

SET enable_parallel_replicas = 0, automatic_parallel_replicas_mode = 0;

SYSTEM FLUSH LOGS query_log;

SELECT ProfileEvents['RuntimeDataflowStatisticsInputBytes'] > 0 AS still_a_candidate
FROM system.query_log
WHERE (event_date >= yesterday()) AND (event_time >= (NOW() - toIntervalMinute(15)))
    AND (current_database = currentDatabase())
    AND (log_comment = '05043_autopr_set_source_leaf_steps')
    AND (type = 'QueryFinish') AND is_initial_query;

DROP TABLE t_autopr_set_source;
