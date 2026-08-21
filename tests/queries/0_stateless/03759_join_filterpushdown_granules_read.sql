DROP TABLE IF EXISTS t_mem;
DROP TABLE IF EXISTS t_mt;

CREATE TABLE t_mem (a Int32, b Int32) ENGINE = Memory;
CREATE TABLE t_mt (a Int32, b Int32) ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 1024, index_granularity_bytes = '10Mi';

INSERT INTO t_mem SELECT number, sipHash64(number, 1) FROM numbers(5_000);
INSERT INTO t_mt SELECT number, sipHash64(number, 2) FROM numbers(5_000);

DROP VIEW IF EXISTS t_view;
CREATE VIEW t_view AS SELECT a, b, 'aaa_' || toString(a) FROM t_mt;

CREATE TEMPORARY TABLE start_ts AS ( SELECT now() AS ts );

SET enable_parallel_replicas = 0;
SET query_plan_join_swap_table = false;
SET enable_analyzer = 1;
SET query_plan_filter_push_down = 1;
SET join_use_nulls = 1;
SET enable_join_runtime_filters = 0;

SELECT * FROM t_view AS t1
LEFT JOIN t_mem AS t2
USING (b)
WHERE t1.a < 2000
SETTINGS log_comment = 'left_join'
FORMAT Null
;

SELECT * FROM t_mem AS t1
RIGHT JOIN t_view AS t2
USING (b)
WHERE t2.a < 2000
SETTINGS log_comment = 'right_join'
FORMAT Null
;

DROP VIEW IF EXISTS left_joined_view;
CREATE VIEW left_joined_view AS
SELECT t1.a as t1_a, t2.a as t2_a
FROM t_mt AS t1
LEFT JOIN t_mem AS t2
USING (b);

SELECT * FROM left_joined_view
WHERE t1_a < 2000
SETTINGS log_comment = 'left_join_view'
FORMAT Null;

DROP VIEW IF EXISTS right_joined_view;
CREATE VIEW right_joined_view AS
SELECT t1.a as t1_a, t2.a as t2_a
FROM t_mem AS t1
RIGHT JOIN t_mt AS t2
USING (b);

SELECT * FROM right_joined_view
WHERE t2_a < 2000
SETTINGS log_comment = 'right_join_view'
FORMAT Null;

SYSTEM FLUSH LOGS system.query_log;

SELECT
    if(ProfileEvents['SelectedMarks'] == 2, 'ok', format('error: {} @ {}', ProfileEvents['SelectedMarks'], query_id)),
    if(ProfileEvents['SelectedMarksTotal'] == 5, 'ok', format('error: {} @ {}', ProfileEvents['SelectedMarksTotal'], query_id)),
    if(ProfileEvents['JoinProbeTableRowCount'] == 2000, 'ok', format('error: {} @ {}', ProfileEvents['JoinProbeTableRowCount'], query_id)),
    if(ProfileEvents['JoinBuildTableRowCount'] == 5000, 'ok', format('error: {} @ {}', ProfileEvents['JoinBuildTableRowCount'], query_id)),
    if(ProfileEvents['JoinResultRowCount'] == 2000, 'ok', format('error: {} @ {}', ProfileEvents['JoinResultRowCount'], query_id)),
FROM system.query_log
WHERE type = 'QueryFinish' AND current_database = currentDatabase()
    AND event_date >= yesterday() AND query_kind = 'Select'
    AND event_time >= (SELECT ts FROM start_ts)
    AND log_comment IN ('left_join', 'left_join_view')
;

SELECT
    if(ProfileEvents['SelectedMarks'] == 2, 'ok', format('error: {} @ {}', ProfileEvents['SelectedMarks'], query_id)),
    if(ProfileEvents['SelectedMarksTotal'] == 5, 'ok', format('error: {} @ {}', ProfileEvents['SelectedMarksTotal'], query_id)),
    if(ProfileEvents['JoinProbeTableRowCount'] == 5000, 'ok', format('error: {} @ {}', ProfileEvents['JoinProbeTableRowCount'], query_id)),
    if(ProfileEvents['JoinBuildTableRowCount'] == 2000, 'ok', format('error: {} @ {}', ProfileEvents['JoinBuildTableRowCount'], query_id)),
    if(ProfileEvents['JoinResultRowCount'] == 2000, 'ok', format('error: {} @ {}', ProfileEvents['JoinResultRowCount'], query_id)),
FROM system.query_log
WHERE type = 'QueryFinish' AND current_database = currentDatabase()
    AND event_date >= yesterday() AND query_kind = 'Select'
    AND event_time >= (SELECT ts FROM start_ts)
    AND log_comment IN ('right_join', 'right_join_view')
;
