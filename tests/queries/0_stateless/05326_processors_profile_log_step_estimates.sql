-- The join order optimizer stamps the row estimate of every relation and every join it decides
-- on the plan node; `EXPLAIN estimates = 1` prints it with the origin of the estimate, and every
-- processor records the estimate of the plan step that created it, so
-- `system.processors_profile_log` can compare it with the rows the step produced. A step's output
-- is the sum of `output_rows` over its processors whose consumers belong to other steps;
-- `parent_ids` lists the consumers of a processor. The hints stand in for column statistics so the
-- numbers are fixed.
SET explain_query_plan_default = 'legacy';
SET enable_analyzer = 1;
SET enable_join_runtime_filters = 0;
-- The plan lines below show the filter moved to PREWHERE; the runner randomizes the move.
SET optimize_move_to_prewhere = 1;
SET enable_parallel_replicas = 0;
SET query_plan_optimize_join_order_limit = 10;
SET query_plan_optimize_join_order_randomize = 0;
SET query_plan_join_swap_table = 0;
SET use_statistics = 0;
SET param__internal_join_table_stat_hints = '{"t_est_a": {"cardinality": 1000, "distinct_keys": {"k": 1000}}, "t_est_b": {"cardinality": 100, "distinct_keys": {"k": 100}}}';
SET log_processors_profiles = 1;

DROP TABLE IF EXISTS t_est_a;
DROP TABLE IF EXISTS t_est_b;

CREATE TABLE t_est_a (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k SETTINGS auto_statistics_types = '';
CREATE TABLE t_est_b (k UInt64, w UInt64) ENGINE = MergeTree ORDER BY k SETTINGS auto_statistics_types = '';
INSERT INTO t_est_a SELECT number, number FROM numbers(1000);
INSERT INTO t_est_b SELECT number * 10, number FROM numbers(100);

SELECT '-- the plan with its estimates';
EXPLAIN estimates = 1
SELECT count()
FROM t_est_a AS a
JOIN t_est_b AS b ON a.k = b.k;

SELECT '-- the same estimates in the profile log, next to the rows produced';
SELECT count()
FROM t_est_a AS a
JOIN t_est_b AS b ON a.k = b.k
SETTINGS log_comment = '05326_step_estimates';

SYSTEM FLUSH LOGS query_log, processors_profile_log;

WITH
    log AS
    (
        SELECT id, parent_ids, step_uniq_id, plan_step_name, plan_step_estimated_rows, plan_step_estimate_source, output_rows
        FROM system.processors_profile_log
        WHERE event_date >= yesterday()
          AND query_id =
          (
              SELECT query_id FROM system.query_log
              WHERE event_date >= yesterday() AND current_database = currentDatabase()
                AND log_comment = '05326_step_estimates' AND type = 'QueryFinish'
              ORDER BY event_time_microseconds DESC LIMIT 1
          )
    ),
    internal AS
    (
        -- processors with at least one consumer inside the same step do not form the step's output
        SELECT l.id AS id
        FROM log AS l
        ARRAY JOIN l.parent_ids AS consumer_id
        INNER JOIN log AS c ON c.id = consumer_id
        WHERE c.step_uniq_id = l.step_uniq_id
        GROUP BY l.id
    )
SELECT plan_step_name, plan_step_estimated_rows, plan_step_estimate_source, sum(output_rows) AS actual_rows
FROM log
WHERE plan_step_estimated_rows IS NOT NULL AND id NOT IN (SELECT id FROM internal)
GROUP BY plan_step_name, plan_step_estimated_rows, plan_step_estimate_source
ORDER BY plan_step_estimated_rows, plan_step_name, plan_step_estimate_source;

SELECT '-- without hints and statistics the relations are estimated from the primary index';
SET param__internal_join_table_stat_hints = '{}';
EXPLAIN estimates = 1
SELECT count()
FROM t_est_a AS a
JOIN t_est_b AS b ON a.k = b.k
WHERE b.w < 10;

DROP TABLE t_est_a;
DROP TABLE t_est_b;
