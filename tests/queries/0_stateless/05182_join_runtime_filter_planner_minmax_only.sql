DROP TABLE IF EXISTS rf_planner_minmax_probe;
DROP TABLE IF EXISTS rf_planner_minmax_build;

CREATE TABLE rf_planner_minmax_probe (k UInt64) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE rf_planner_minmax_build (k UInt64 STATISTICS(uniq)) ENGINE = MergeTree ORDER BY tuple();

INSERT INTO rf_planner_minmax_probe SELECT number FROM numbers(10000);
INSERT INTO rf_planner_minmax_build SELECT number + 4000 FROM numbers(2000);
ALTER TABLE rf_planner_minmax_build MATERIALIZE STATISTICS ALL SETTINGS mutations_sync = 1;

SET enable_analyzer = 1;
SET enable_join_runtime_filters = 1;
SET enable_join_fixed_hash_table_conversion = 0;
SET explain_query_plan_default = 'legacy';
SET join_algorithm = 'hash';
SET join_runtime_filter_exact_values_limit = 0;
SET join_runtime_filter_from_fixed_hash_table = 0;
SET join_runtime_filter_min_probe_rows = 0;
SET join_runtime_filter_pass_ratio_threshold_for_disabling = 1;
SET join_runtime_filter_size_from_hash_table_stats = 0;
SET join_runtime_filter_use_minmax = 1;
SET join_runtime_bloom_filter_bytes = 128;
SET join_runtime_bloom_filter_max_estimated_ratio_of_set_bits = 0.01;
SET query_plan_join_swap_table = 0;
SET query_plan_optimize_join_order_limit = 1;
SET use_statistics = 1;

SELECT count()
FROM
(
    EXPLAIN PLAN
    SELECT count()
    FROM rf_planner_minmax_probe AS p
    INNER JOIN rf_planner_minmax_build AS b ON p.k = b.k
)
WHERE explain LIKE '%Build minmax-only runtime join filter%';

SELECT count()
FROM rf_planner_minmax_probe AS p
INNER JOIN rf_planner_minmax_build AS b ON p.k = b.k;

SYSTEM FLUSH LOGS query_log;
SELECT
    ProfileEvents['RuntimeFiltersCreated'] > 0,
    ProfileEvents['RuntimeFilterRowsChecked'] > 0,
    ProfileEvents['RuntimeFilterRowsPassed'] < ProfileEvents['RuntimeFilterRowsChecked']
FROM system.query_log
WHERE current_database = currentDatabase()
    AND type = 'QueryFinish'
    AND query LIKE '%SELECT count()%rf_planner_minmax_probe%rf_planner_minmax_build%'
    AND query NOT LIKE '%system.query_log%'
ORDER BY event_time_microseconds DESC
LIMIT 1;

DROP TABLE rf_planner_minmax_probe;
DROP TABLE rf_planner_minmax_build;
