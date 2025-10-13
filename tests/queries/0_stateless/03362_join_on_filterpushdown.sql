-- Tags: no-parallel-replicas

SET enable_parallel_replicas = 0;
SET query_plan_join_swap_table = false;
SET enable_analyzer = 1;
SET query_plan_filter_push_down = 1;

SELECT *
FROM (SELECT number AS key, number AS value FROM numbers(100)) t1
LEFT JOIN (SELECT number AS key, number AS value FROM numbers(100)) t2
ON t1.key = t2.key
    AND t1.value < 50
    AND t2.value < 50
FORMAT Null
SETTINGS log_comment = '03362_join_on_filterpushdown_left'
;


SELECT *
FROM (SELECT number AS key, number AS value FROM numbers(100)) t1
LEFT JOIN (SELECT number AS key, number AS value FROM numbers(100)) t2
ON t1.key = t2.key
WHERE t1.value < 50
    AND t2.value < 50
FORMAT Null
SETTINGS log_comment = '03362_join_on_filterpushdown_left_where'
;

SELECT *
FROM (SELECT number AS key, number AS value FROM numbers(100)) t1
LEFT JOIN (SELECT number AS key, number AS value FROM numbers(100)) t2
ON t1.key = t2.key
WHERE t1.value >= 50
    AND t2.value >= 50
FORMAT Null
SETTINGS log_comment = '03362_join_on_filterpushdown_left_where_filter_zeros'
;

SELECT *
FROM (SELECT number AS key, number AS value FROM numbers(100)) t1
RIGHT JOIN (SELECT number AS key, number AS value FROM numbers(100)) t2
ON t1.key = t2.key
    AND t1.value < 50
    AND t2.value < 50
FORMAT Null
SETTINGS log_comment = '03362_join_on_filterpushdown_right'
;

SELECT *
FROM (SELECT number AS key, number AS value FROM numbers(100)) t1
JOIN (SELECT number AS key, number AS value FROM numbers(100)) t2
ON t1.key = t2.key
    AND t1.value < 50
    AND t2.value < 50
FORMAT Null
SETTINGS log_comment = '03362_join_on_filterpushdown_inner'
;


SELECT *
FROM (SELECT number AS key, number AS value FROM numbers(100)) t1
FULL JOIN (SELECT number AS key, number AS value FROM numbers(100)) t2
ON t1.key = t2.key
    AND t1.value < 50
    AND t2.value < 50
FORMAT Null
SETTINGS log_comment = '03362_join_on_filterpushdown_full'
;

SYSTEM FLUSH LOGS query_log;

SELECT
    if(ProfileEvents['JoinProbeTableRowCount'] == 100, 'ok', 'fail: ' || toString(ProfileEvents['JoinProbeTableRowCount'])),
    if(ProfileEvents['JoinBuildTableRowCount'] == 50, 'ok', 'fail: ' || toString(ProfileEvents['JoinBuildTableRowCount'])),
    if(ProfileEvents['JoinResultRowCount'] == 100, 'ok', 'fail: ' || toString(ProfileEvents['JoinResultRowCount'])),
FROM system.query_log
WHERE type = 'QueryFinish' AND event_date >= yesterday() AND query_kind = 'Select' AND current_database = currentDatabase()
AND log_comment = '03362_join_on_filterpushdown_left'
ORDER BY event_time DESC
LIMIT 1;


SELECT
    if(ProfileEvents['JoinProbeTableRowCount'] == 50, 'ok', 'fail: ' || toString(ProfileEvents['JoinProbeTableRowCount'])),
    if(ProfileEvents['JoinBuildTableRowCount'] == 100, 'ok', 'fail: ' || toString(ProfileEvents['JoinBuildTableRowCount'])),
    if(ProfileEvents['JoinResultRowCount'] == 50, 'ok', 'fail: ' || toString(ProfileEvents['JoinResultRowCount'])),
FROM system.query_log
WHERE type = 'QueryFinish' AND event_date >= yesterday() AND query_kind = 'Select' AND current_database = currentDatabase()
AND log_comment = '03362_join_on_filterpushdown_left_where'
ORDER BY event_time DESC
LIMIT 1;

SELECT
    if(ProfileEvents['JoinProbeTableRowCount'] == 50, 'ok', 'fail: ' || toString(ProfileEvents['JoinProbeTableRowCount'])),
    if(ProfileEvents['JoinBuildTableRowCount'] == 50, 'ok', 'fail: ' || toString(ProfileEvents['JoinBuildTableRowCount'])),
    if(ProfileEvents['JoinResultRowCount'] == 50, 'ok', 'fail: ' || toString(ProfileEvents['JoinResultRowCount'])),
FROM system.query_log
WHERE type = 'QueryFinish' AND event_date >= yesterday() AND query_kind = 'Select' AND current_database = currentDatabase()
AND log_comment = '03362_join_on_filterpushdown_left_where_filter_zeros'
ORDER BY event_time DESC
LIMIT 1;

SELECT
    if(ProfileEvents['JoinProbeTableRowCount'] == 50, 'ok', 'fail: ' || toString(ProfileEvents['JoinProbeTableRowCount'])),
    if(ProfileEvents['JoinBuildTableRowCount'] == 100, 'ok', 'fail: ' || toString(ProfileEvents['JoinBuildTableRowCount'])),
    if(ProfileEvents['JoinResultRowCount'] == 100, 'ok', 'fail: ' || toString(ProfileEvents['JoinResultRowCount'])),
FROM system.query_log
WHERE type = 'QueryFinish' AND event_date >= yesterday() AND query_kind = 'Select' AND current_database = currentDatabase()
AND log_comment = '03362_join_on_filterpushdown_right'
ORDER BY event_time DESC
LIMIT 1;


SELECT
    if(ProfileEvents['JoinProbeTableRowCount'] == 50, 'ok', 'fail: ' || toString(ProfileEvents['JoinProbeTableRowCount'])),
    if(ProfileEvents['JoinBuildTableRowCount'] == 50, 'ok', 'fail: ' || toString(ProfileEvents['JoinBuildTableRowCount'])),
    if(ProfileEvents['JoinResultRowCount'] == 50, 'ok', 'fail: ' || toString(ProfileEvents['JoinResultRowCount'])),
FROM system.query_log
WHERE type = 'QueryFinish' AND event_date >= yesterday() AND query_kind = 'Select' AND current_database = currentDatabase()
AND log_comment = '03362_join_on_filterpushdown_inner'
ORDER BY event_time DESC
LIMIT 1;

SELECT
    if(ProfileEvents['JoinProbeTableRowCount'] == 100, 'ok', 'fail: ' || toString(ProfileEvents['JoinProbeTableRowCount'])),
    if(ProfileEvents['JoinBuildTableRowCount'] == 100, 'ok', 'fail: ' || toString(ProfileEvents['JoinBuildTableRowCount'])),
    if(ProfileEvents['JoinResultRowCount'] == 150, 'ok', 'fail: ' || toString(ProfileEvents['JoinResultRowCount'])),
FROM system.query_log
WHERE type = 'QueryFinish' AND event_date >= yesterday() AND query_kind = 'Select' AND current_database = currentDatabase()
AND log_comment = '03362_join_on_filterpushdown_full'
ORDER BY event_time DESC
LIMIT 1;

