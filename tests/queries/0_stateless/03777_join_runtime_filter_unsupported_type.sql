-- Freeze some settings to produce stable query plans
SET enable_analyzer=1;
SET enable_parallel_replicas=0;
SET join_algorithm = 'hash,parallel_hash';
SET query_plan_optimize_join_order_algorithm='greedy';
SET query_plan_optimize_join_order_limit=1;
SET query_plan_join_swap_table=0;


SELECT '============ Filter key count greater than exact values limit';
SELECT *
FROM
    (SELECT (number, number+1) AS a FROM numbers(1000-3, 3)) AS t1,
    (SELECT (number, number+1) aS b FROM numbers(1000)) AS t2
WHERE t1.a = t2.b
SETTINGS enable_join_runtime_filters=1, join_runtime_filter_exact_values_limit=100, log_comment='Q1';

SYSTEM FLUSH LOGS system.query_log;

SELECT ProfileEvents['RuntimeFilterRowsChecked'], ProfileEvents['RuntimeFilterRowsSkipped']
FROM system.query_log
WHERE current_database = currentDatabase() AND event_time > now() - INTERVAL 30 MINUTE AND type = 'QueryFinish' AND log_comment = 'Q1';


SELECT '============ Filter key count less than exact values limit';
SELECT *
FROM
    (SELECT (number, number+1) AS a FROM numbers(1000-3, 3)) AS t1,
    (SELECT (number, number+1) aS b FROM numbers(1000)) AS t2
WHERE t1.a = t2.b
SETTINGS enable_join_runtime_filters=1, join_runtime_filter_exact_values_limit=10000, log_comment='Q2';

SYSTEM FLUSH LOGS system.query_log;

SELECT ProfileEvents['RuntimeFilterRowsChecked'], ProfileEvents['RuntimeFilterRowsSkipped']
FROM system.query_log
WHERE current_database = currentDatabase() AND event_time > now() - INTERVAL 30 MINUTE AND type = 'QueryFinish' AND log_comment = 'Q2';
