-- A distributed plan is serialized into fragments the tasks execute. The fragments carry the
-- optimizer's step estimates, so `system.processors_profile_log` shows them for the processors of
-- every task, not only for the ones the coordinator runs itself. Without this the worker-side rows
-- of the log had no estimate at all.
SET enable_analyzer = 1;
SET enable_parallel_replicas = 0;
SET enable_cascades_optimizer = 0;
SET enable_join_runtime_filters = 0;
SET query_plan_join_swap_table = 0;
SET query_plan_optimize_join_order_randomize = 0;
SET distributed_plan_default_shuffle_join_bucket_count = 2;
SET distributed_plan_default_reader_bucket_count = 2;
SET max_rows_to_group_by = 0;
SET use_statistics = 0;
SET log_processors_profiles = 1;
SET log_queries = 1;
SET param__internal_join_table_stat_hints = '{"t_fe_fact": {"cardinality": 100000, "distinct_keys": {"k": 1000}}, "t_fe_dim": {"cardinality": 1000, "distinct_keys": {"k": 1000}}}';

DROP TABLE IF EXISTS t_fe_fact;
DROP TABLE IF EXISTS t_fe_dim;
CREATE TABLE t_fe_fact (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k SETTINGS auto_statistics_types = '';
CREATE TABLE t_fe_dim (k UInt64) ENGINE = MergeTree ORDER BY k SETTINGS auto_statistics_types = '';
INSERT INTO t_fe_fact SELECT number % 1000, number FROM numbers(100000);
INSERT INTO t_fe_dim SELECT number FROM numbers(1000);

-- Only this query takes the distributed plan: the queries of the system logs below read tables whose
-- parts merge while they run.
SELECT count() FROM t_fe_fact AS f JOIN t_fe_dim AS d ON f.k = d.k
SETTINGS make_distributed_plan = 1, distributed_plan_execute_locally = 1, log_comment = '05355_fragment_estimates';
SYSTEM FLUSH LOGS query_log, processors_profile_log;

-- The join runs in the tasks: their processors are logged under their own query ids. A read's
-- estimate sits on the expression above it, as in `EXPLAIN estimates = 1`.
SELECT plan_step_name, plan_step_estimated_rows AS estimated_rows, plan_step_estimate_source AS source, count() > 0 AS logged
FROM system.processors_profile_log
WHERE event_date >= yesterday()
  AND initial_query_id = (
      SELECT query_id FROM system.query_log
      WHERE event_date >= yesterday() AND current_database = currentDatabase()
        AND log_comment = '05355_fragment_estimates' AND type = 'QueryFinish'
      ORDER BY event_time_microseconds DESC LIMIT 1)
  AND query_id != initial_query_id
  AND plan_step_name IN ('JoinLogical', 'Join', 'Expression')
GROUP BY plan_step_name, estimated_rows, source
ORDER BY plan_step_name, estimated_rows, source;

DROP TABLE t_fe_fact;
DROP TABLE t_fe_dim;
