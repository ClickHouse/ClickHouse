-- Plan-based parallel replicas ships the join with its runtime filter, which must filter on the remote
-- replicas too. The results must match the non-parallel ones; the row counts are checked only for INNER
-- and SEMI, as the ANTI filter keeps almost all rows.

SET enable_analyzer = 1;
SET enable_parallel_replicas = 1;
SET automatic_parallel_replicas_mode = 0;
SET max_parallel_replicas = 3;
SET cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';
SET parallel_replicas_for_non_replicated_merge_tree = 1;
SET parallel_replicas_plan_based = 1;
-- No local plan, so the remote replicas do all the reading, which the assertions below check.
SET parallel_replicas_local_plan = 0;
SET parallel_replicas_min_number_of_rows_per_replica = 0;
SET enable_join_runtime_filters = 1;
SET join_runtime_filter_min_probe_rows = 0;
SET query_plan_optimize_join_order_randomize = 0;
SET query_plan_join_swap_table = 'false';

DROP TABLE IF EXISTS rf_probe;
DROP TABLE IF EXISTS rf_build;

CREATE TABLE rf_probe (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 128;
CREATE TABLE rf_build (k UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO rf_probe SELECT number, number FROM numbers(100000);
INSERT INTO rf_build SELECT number * 1000 FROM numbers(50);

SELECT 'inner', count(), sum(v) FROM rf_probe AS p INNER JOIN rf_build AS b ON p.k = b.k SETTINGS log_comment = '05367_inner';
SELECT 'semi', count(), sum(v) FROM rf_probe AS p LEFT SEMI JOIN rf_build AS b ON p.k = b.k SETTINGS log_comment = '05367_semi';
SELECT 'anti', count(), sum(v) FROM rf_probe AS p LEFT ANTI JOIN rf_build AS b ON p.k = b.k;

SELECT 'inner', count(), sum(v) FROM rf_probe AS p INNER JOIN rf_build AS b ON p.k = b.k SETTINGS enable_parallel_replicas = 0;
SELECT 'semi', count(), sum(v) FROM rf_probe AS p LEFT SEMI JOIN rf_build AS b ON p.k = b.k SETTINGS enable_parallel_replicas = 0;
SELECT 'anti', count(), sum(v) FROM rf_probe AS p LEFT ANTI JOIN rf_build AS b ON p.k = b.k SETTINGS enable_parallel_replicas = 0;

SYSTEM FLUSH LOGS query_log, processors_profile_log;

-- The reads of `rf_probe` on the remote replicas output only the 50 matching rows plus a few bloom filter
-- false positives. Without the shipped key the read outputs 6784 rows for INNER.
SELECT log_comment, count() > 0, sum(probe_rows) BETWEEN 50 AND 500
FROM
(
    SELECT query_id, log_comment
    FROM system.query_log
    WHERE event_date >= yesterday() AND type = 'QueryFinish' AND NOT is_initial_query
      AND initial_query_id IN (SELECT argMax(query_id, event_time_microseconds) FROM system.query_log
                               WHERE event_date >= yesterday() AND current_database = currentDatabase()
                                 AND log_comment IN ('05367_inner', '05367_semi') AND type = 'QueryFinish' AND is_initial_query
                               GROUP BY log_comment)
) AS tasks
INNER JOIN
(
    SELECT query_id, sum(output_rows) AS probe_rows
    FROM system.processors_profile_log
    WHERE event_date >= yesterday() AND name LIKE 'MergeTreeSelect%' AND plan_step_description = currentDatabase() || '.rf_probe'
    GROUP BY query_id
) AS reads ON reads.query_id = tasks.query_id
GROUP BY log_comment
ORDER BY log_comment;

DROP TABLE rf_probe;
DROP TABLE rf_build;
