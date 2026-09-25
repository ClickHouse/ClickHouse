DROP VIEW IF EXISTS t05239_non_neutral;
DROP VIEW IF EXISTS t05239_transformed;
DROP VIEW IF EXISTS t05239_view;
DROP TABLE IF EXISTS t05239_expected;
DROP TABLE IF EXISTS t05239_default;
DROP TABLE IF EXISTS t05239_merge;
DROP TABLE IF EXISTS t05239_keys;
DROP TABLE IF EXISTS t05239_values;

SET max_threads = 2;
-- Exercise rewrite semantics on small fixtures independently of the default cost gate.
SET optimize_merge_neutral_sum_children_min_read_bytes = 0;
SET log_queries = 1;
SET log_queries_min_type = 'QUERY_FINISH';
SET optimize_use_projections = 1;
SET optimize_distinct_in_order = 1;
CREATE TABLE t05239_values (cob Date, desk String, trader String, pnl Nullable(Float64), delta Float64)
ENGINE = MergeTree ORDER BY cob;
CREATE TABLE t05239_keys (cob Date, desk String, trader String, delta Float64,
PROJECTION p (SELECT cob, desk, trader, sum(delta) GROUP BY cob, desk, trader))
ENGINE = MergeTree ORDER BY cob;
SYSTEM STOP MERGES t05239_keys;
INSERT INTO t05239_values VALUES ('2026-09-08','0','0',10,1), ('2026-09-08','0','0',20,-1),
('2026-09-09','only_values','0',7,1), ('2026-09-10','null_value','0',NULL,1);
INSERT INTO t05239_keys SELECT toDate('2026-09-08') + number % 3, toString(number % 2),
toString(number % 5), if(number % 7 = 0, -1., 1.) FROM numbers(100000);
INSERT INTO t05239_keys SELECT toDate('2026-09-08') + number % 3, toString(number % 2),
toString(number % 5), if(number % 7 = 0, -1., 1.) FROM numbers(100000);
CREATE TABLE t05239_merge AS t05239_values ENGINE = Merge(currentDatabase(), '^t05239_(values|keys)$');
CREATE TABLE t05239_default (cob Date, desk String, trader String, pnl Nullable(Float64) DEFAULT 7)
ENGINE = Merge(currentDatabase(), '^t05239_(values|keys)$');
CREATE TABLE t05239_expected (rows String) ENGINE = Memory;

-- all: compare every grouping key, aggregate value, NULL flag and type.
INSERT INTO t05239_expected SELECT toString(arraySort(groupArray(tuple(cob,desk,trader,isNull(s),ifNull(s,0),toTypeName(s))))) FROM (SELECT cob,desk,trader,sum(pnl) s FROM t05239_merge  GROUP BY cob,desk,trader)
SETTINGS optimize_merge_neutral_sum_children=0, log_comment='t05239_all_off';
SELECT 'all', (SELECT toString(arraySort(groupArray(tuple(cob,desk,trader,isNull(s),ifNull(s,0),toTypeName(s))))) FROM (SELECT cob,desk,trader,sum(pnl) s FROM t05239_merge  GROUP BY cob,desk,trader)) = (SELECT rows FROM t05239_expected)
SETTINGS optimize_merge_neutral_sum_children=1, log_comment='t05239_all';
TRUNCATE TABLE t05239_expected;
SYSTEM FLUSH LOGS;
SELECT 'all_projection', notEmpty(projections) = 1
FROM system.user_query_log
WHERE current_database=currentDatabase() AND type='QueryFinish' AND log_comment='t05239_all'
ORDER BY event_time_microseconds DESC LIMIT 1;
SELECT 'all_reads', (SELECT read_rows
FROM system.user_query_log
WHERE current_database=currentDatabase() AND type='QueryFinish' AND log_comment='t05239_all'
ORDER BY event_time_microseconds DESC LIMIT 1) < (SELECT read_rows
FROM system.user_query_log
WHERE current_database=currentDatabase() AND type='QueryFinish' AND log_comment='t05239_all_off'
ORDER BY event_time_microseconds DESC LIMIT 1) / 10;

-- eq: compare every grouping key, aggregate value, NULL flag and type.
INSERT INTO t05239_expected SELECT toString(arraySort(groupArray(tuple(cob,desk,trader,isNull(s),ifNull(s,0),toTypeName(s))))) FROM (SELECT cob,desk,trader,sum(pnl) s FROM t05239_merge WHERE cob = '2026-09-08' GROUP BY cob,desk,trader)
SETTINGS optimize_merge_neutral_sum_children=0, log_comment='t05239_eq_off';
SELECT 'eq', (SELECT toString(arraySort(groupArray(tuple(cob,desk,trader,isNull(s),ifNull(s,0),toTypeName(s))))) FROM (SELECT cob,desk,trader,sum(pnl) s FROM t05239_merge WHERE cob = '2026-09-08' GROUP BY cob,desk,trader)) = (SELECT rows FROM t05239_expected)
SETTINGS optimize_merge_neutral_sum_children=1, log_comment='t05239_eq';
TRUNCATE TABLE t05239_expected;
SYSTEM FLUSH LOGS;
SELECT 'eq_projection', notEmpty(projections) = 1
FROM system.user_query_log
WHERE current_database=currentDatabase() AND type='QueryFinish' AND log_comment='t05239_eq'
ORDER BY event_time_microseconds DESC LIMIT 1;
SELECT 'eq_reads', (SELECT read_rows
FROM system.user_query_log
WHERE current_database=currentDatabase() AND type='QueryFinish' AND log_comment='t05239_eq'
ORDER BY event_time_microseconds DESC LIMIT 1) < (SELECT read_rows
FROM system.user_query_log
WHERE current_database=currentDatabase() AND type='QueryFinish' AND log_comment='t05239_eq_off'
ORDER BY event_time_microseconds DESC LIMIT 1) / 10;

-- range: compare every grouping key, aggregate value, NULL flag and type.
INSERT INTO t05239_expected SELECT toString(arraySort(groupArray(tuple(cob,desk,trader,isNull(s),ifNull(s,0),toTypeName(s))))) FROM (SELECT cob,desk,trader,sum(pnl) s FROM t05239_merge WHERE cob >= '2026-09-09' AND cob < '2026-09-11' GROUP BY cob,desk,trader)
SETTINGS optimize_merge_neutral_sum_children=0, log_comment='t05239_range_off';
SELECT 'range', (SELECT toString(arraySort(groupArray(tuple(cob,desk,trader,isNull(s),ifNull(s,0),toTypeName(s))))) FROM (SELECT cob,desk,trader,sum(pnl) s FROM t05239_merge WHERE cob >= '2026-09-09' AND cob < '2026-09-11' GROUP BY cob,desk,trader)) = (SELECT rows FROM t05239_expected)
SETTINGS optimize_merge_neutral_sum_children=1, log_comment='t05239_range';
TRUNCATE TABLE t05239_expected;
SYSTEM FLUSH LOGS;
SELECT 'range_projection', notEmpty(projections) = 1
FROM system.user_query_log
WHERE current_database=currentDatabase() AND type='QueryFinish' AND log_comment='t05239_range'
ORDER BY event_time_microseconds DESC LIMIT 1;
SELECT 'range_reads', (SELECT read_rows
FROM system.user_query_log
WHERE current_database=currentDatabase() AND type='QueryFinish' AND log_comment='t05239_range'
ORDER BY event_time_microseconds DESC LIMIT 1) < (SELECT read_rows
FROM system.user_query_log
WHERE current_database=currentDatabase() AND type='QueryFinish' AND log_comment='t05239_range_off'
ORDER BY event_time_microseconds DESC LIMIT 1) / 10;

-- in: compare every grouping key, aggregate value, NULL flag and type.
INSERT INTO t05239_expected SELECT toString(arraySort(groupArray(tuple(cob,desk,trader,isNull(s),ifNull(s,0),toTypeName(s))))) FROM (SELECT cob,desk,trader,sum(pnl) s FROM t05239_merge WHERE cob IN ('2026-09-08','2026-09-10') AND desk = '0' GROUP BY cob,desk,trader)
SETTINGS optimize_merge_neutral_sum_children=0, log_comment='t05239_in_off';
SELECT 'in', (SELECT toString(arraySort(groupArray(tuple(cob,desk,trader,isNull(s),ifNull(s,0),toTypeName(s))))) FROM (SELECT cob,desk,trader,sum(pnl) s FROM t05239_merge WHERE cob IN ('2026-09-08','2026-09-10') AND desk = '0' GROUP BY cob,desk,trader)) = (SELECT rows FROM t05239_expected)
SETTINGS optimize_merge_neutral_sum_children=1, log_comment='t05239_in';
TRUNCATE TABLE t05239_expected;
SYSTEM FLUSH LOGS;
SELECT 'in_projection', notEmpty(projections) = 1
FROM system.user_query_log
WHERE current_database=currentDatabase() AND type='QueryFinish' AND log_comment='t05239_in'
ORDER BY event_time_microseconds DESC LIMIT 1;
SELECT 'in_reads', (SELECT read_rows
FROM system.user_query_log
WHERE current_database=currentDatabase() AND type='QueryFinish' AND log_comment='t05239_in'
ORDER BY event_time_microseconds DESC LIMIT 1) < (SELECT read_rows
FROM system.user_query_log
WHERE current_database=currentDatabase() AND type='QueryFinish' AND log_comment='t05239_in_off'
ORDER BY event_time_microseconds DESC LIMIT 1) / 10;

-- no_prewhere: compare every grouping key, aggregate value, NULL flag and type.
INSERT INTO t05239_expected SELECT toString(arraySort(groupArray(tuple(cob,desk,trader,isNull(s),ifNull(s,0),toTypeName(s))))) FROM (SELECT cob,desk,trader,sum(pnl) s FROM t05239_merge WHERE cob = '2026-09-08' AND desk = '0' GROUP BY cob,desk,trader)
SETTINGS optimize_merge_neutral_sum_children=0, log_comment='t05239_no_prewhere_off', optimize_move_to_prewhere=0;
SELECT 'no_prewhere', (SELECT toString(arraySort(groupArray(tuple(cob,desk,trader,isNull(s),ifNull(s,0),toTypeName(s))))) FROM (SELECT cob,desk,trader,sum(pnl) s FROM t05239_merge WHERE cob = '2026-09-08' AND desk = '0' GROUP BY cob,desk,trader)) = (SELECT rows FROM t05239_expected)
SETTINGS optimize_merge_neutral_sum_children=1, log_comment='t05239_no_prewhere', optimize_move_to_prewhere=0;
TRUNCATE TABLE t05239_expected;
SYSTEM FLUSH LOGS;
SELECT 'no_prewhere_projection', notEmpty(projections) = 1
FROM system.user_query_log
WHERE current_database=currentDatabase() AND type='QueryFinish' AND log_comment='t05239_no_prewhere'
ORDER BY event_time_microseconds DESC LIMIT 1;
SELECT 'no_prewhere_reads', (SELECT read_rows
FROM system.user_query_log
WHERE current_database=currentDatabase() AND type='QueryFinish' AND log_comment='t05239_no_prewhere'
ORDER BY event_time_microseconds DESC LIMIT 1) < (SELECT read_rows
FROM system.user_query_log
WHERE current_database=currentDatabase() AND type='QueryFinish' AND log_comment='t05239_no_prewhere_off'
ORDER BY event_time_microseconds DESC LIMIT 1) / 10;

-- explicit: compare every grouping key, aggregate value, NULL flag and type.
INSERT INTO t05239_expected SELECT toString(arraySort(groupArray(tuple(cob,desk,trader,isNull(s),ifNull(s,0),toTypeName(s))))) FROM (SELECT cob,desk,trader,sum(pnl) s FROM t05239_merge PREWHERE cob = '2026-09-08' AND desk = '0' GROUP BY cob,desk,trader)
SETTINGS optimize_merge_neutral_sum_children=0, log_comment='t05239_explicit_off';
SELECT 'explicit', (SELECT toString(arraySort(groupArray(tuple(cob,desk,trader,isNull(s),ifNull(s,0),toTypeName(s))))) FROM (SELECT cob,desk,trader,sum(pnl) s FROM t05239_merge PREWHERE cob = '2026-09-08' AND desk = '0' GROUP BY cob,desk,trader)) = (SELECT rows FROM t05239_expected)
SETTINGS optimize_merge_neutral_sum_children=1, log_comment='t05239_explicit';
TRUNCATE TABLE t05239_expected;
SYSTEM FLUSH LOGS;
SELECT 'explicit_projection', notEmpty(projections) = 1
FROM system.user_query_log
WHERE current_database=currentDatabase() AND type='QueryFinish' AND log_comment='t05239_explicit'
ORDER BY event_time_microseconds DESC LIMIT 1;
SELECT 'explicit_reads', (SELECT read_rows
FROM system.user_query_log
WHERE current_database=currentDatabase() AND type='QueryFinish' AND log_comment='t05239_explicit'
ORDER BY event_time_microseconds DESC LIMIT 1) < (SELECT read_rows
FROM system.user_query_log
WHERE current_database=currentDatabase() AND type='QueryFinish' AND log_comment='t05239_explicit_off'
ORDER BY event_time_microseconds DESC LIMIT 1) / 10;

-- expression: compare every grouping key, aggregate value, NULL flag and type.
INSERT INTO t05239_expected SELECT toString(arraySort(groupArray(tuple(cob,desk,trader,isNull(s),ifNull(s,0),toTypeName(s))))) FROM (SELECT cob,desk,trader,sum(pnl) s FROM t05239_merge WHERE length(desk) = 1 AND cob = '2026-09-08' GROUP BY cob,desk,trader)
SETTINGS optimize_merge_neutral_sum_children=0, log_comment='t05239_expression_off';
SELECT 'expression', (SELECT toString(arraySort(groupArray(tuple(cob,desk,trader,isNull(s),ifNull(s,0),toTypeName(s))))) FROM (SELECT cob,desk,trader,sum(pnl) s FROM t05239_merge WHERE length(desk) = 1 AND cob = '2026-09-08' GROUP BY cob,desk,trader)) = (SELECT rows FROM t05239_expected)
SETTINGS optimize_merge_neutral_sum_children=1, log_comment='t05239_expression';
TRUNCATE TABLE t05239_expected;
SYSTEM FLUSH LOGS;
SELECT 'expression_projection', notEmpty(projections) = 1
FROM system.user_query_log
WHERE current_database=currentDatabase() AND type='QueryFinish' AND log_comment='t05239_expression'
ORDER BY event_time_microseconds DESC LIMIT 1;
SELECT 'expression_reads', (SELECT read_rows
FROM system.user_query_log
WHERE current_database=currentDatabase() AND type='QueryFinish' AND log_comment='t05239_expression'
ORDER BY event_time_microseconds DESC LIMIT 1) < (SELECT read_rows
FROM system.user_query_log
WHERE current_database=currentDatabase() AND type='QueryFinish' AND log_comment='t05239_expression_off'
ORDER BY event_time_microseconds DESC LIMIT 1) / 10;

-- empty: compare every grouping key, aggregate value, NULL flag and type.
INSERT INTO t05239_expected SELECT toString(arraySort(groupArray(tuple(cob,desk,trader,isNull(s),ifNull(s,0),toTypeName(s))))) FROM (SELECT cob,desk,trader,sum(pnl) s FROM t05239_merge WHERE cob = '2030-01-01' GROUP BY cob,desk,trader)
SETTINGS optimize_merge_neutral_sum_children=0, log_comment='t05239_empty_off';
SELECT 'empty', (SELECT toString(arraySort(groupArray(tuple(cob,desk,trader,isNull(s),ifNull(s,0),toTypeName(s))))) FROM (SELECT cob,desk,trader,sum(pnl) s FROM t05239_merge WHERE cob = '2030-01-01' GROUP BY cob,desk,trader)) = (SELECT rows FROM t05239_expected)
SETTINGS optimize_merge_neutral_sum_children=1, log_comment='t05239_empty';
TRUNCATE TABLE t05239_expected;
SYSTEM FLUSH LOGS;
SELECT 'empty_projection', notEmpty(projections) = 1
FROM system.user_query_log
WHERE current_database=currentDatabase() AND type='QueryFinish' AND log_comment='t05239_empty'
ORDER BY event_time_microseconds DESC LIMIT 1;

-- neutral_only: compare every grouping key, aggregate value, NULL flag and type.
INSERT INTO t05239_expected SELECT toString(arraySort(groupArray(tuple(cob,desk,trader,isNull(s),ifNull(s,0),toTypeName(s))))) FROM (SELECT cob,desk,trader,sum(pnl) s FROM t05239_merge WHERE desk = '1' AND cob = '2026-09-10' GROUP BY cob,desk,trader)
SETTINGS optimize_merge_neutral_sum_children=0, log_comment='t05239_neutral_only_off';
SELECT 'neutral_only', (SELECT toString(arraySort(groupArray(tuple(cob,desk,trader,isNull(s),ifNull(s,0),toTypeName(s))))) FROM (SELECT cob,desk,trader,sum(pnl) s FROM t05239_merge WHERE desk = '1' AND cob = '2026-09-10' GROUP BY cob,desk,trader)) = (SELECT rows FROM t05239_expected)
SETTINGS optimize_merge_neutral_sum_children=1, log_comment='t05239_neutral_only';
TRUNCATE TABLE t05239_expected;
SYSTEM FLUSH LOGS;
SELECT 'neutral_only_projection', notEmpty(projections) = 1
FROM system.user_query_log
WHERE current_database=currentDatabase() AND type='QueryFinish' AND log_comment='t05239_neutral_only'
ORDER BY event_time_microseconds DESC LIMIT 1;
SELECT 'neutral_only_reads', (SELECT read_rows
FROM system.user_query_log
WHERE current_database=currentDatabase() AND type='QueryFinish' AND log_comment='t05239_neutral_only'
ORDER BY event_time_microseconds DESC LIMIT 1) < (SELECT read_rows
FROM system.user_query_log
WHERE current_database=currentDatabase() AND type='QueryFinish' AND log_comment='t05239_neutral_only_off'
ORDER BY event_time_microseconds DESC LIMIT 1) / 10;

-- measure: compare every grouping key, aggregate value, NULL flag and type.
INSERT INTO t05239_expected SELECT toString(arraySort(groupArray(tuple(cob,desk,trader,isNull(s),ifNull(s,0),toTypeName(s))))) FROM (SELECT cob,desk,trader,sum(pnl) s FROM t05239_merge WHERE pnl > 0 GROUP BY cob,desk,trader)
SETTINGS optimize_merge_neutral_sum_children=0, log_comment='t05239_measure_off';
SELECT 'measure', (SELECT toString(arraySort(groupArray(tuple(cob,desk,trader,isNull(s),ifNull(s,0),toTypeName(s))))) FROM (SELECT cob,desk,trader,sum(pnl) s FROM t05239_merge WHERE pnl > 0 GROUP BY cob,desk,trader)) = (SELECT rows FROM t05239_expected)
SETTINGS optimize_merge_neutral_sum_children=1, log_comment='t05239_measure';
TRUNCATE TABLE t05239_expected;
SYSTEM FLUSH LOGS;
SELECT 'measure_projection', notEmpty(projections) = 0
FROM system.user_query_log
WHERE current_database=currentDatabase() AND type='QueryFinish' AND log_comment='t05239_measure'
ORDER BY event_time_microseconds DESC LIMIT 1;

-- row_level: compare every grouping key, aggregate value, NULL flag and type.
INSERT INTO t05239_expected SELECT toString(arraySort(groupArray(tuple(cob,desk,trader,isNull(s),ifNull(s,0),toTypeName(s))))) FROM (SELECT cob,desk,trader,sum(pnl) s FROM t05239_merge WHERE delta > 0 GROUP BY cob,desk,trader)
SETTINGS optimize_merge_neutral_sum_children=0, log_comment='t05239_row_level_off';
SELECT 'row_level', (SELECT toString(arraySort(groupArray(tuple(cob,desk,trader,isNull(s),ifNull(s,0),toTypeName(s))))) FROM (SELECT cob,desk,trader,sum(pnl) s FROM t05239_merge WHERE delta > 0 GROUP BY cob,desk,trader)) = (SELECT rows FROM t05239_expected)
SETTINGS optimize_merge_neutral_sum_children=1, log_comment='t05239_row_level';
TRUNCATE TABLE t05239_expected;
SYSTEM FLUSH LOGS;
SELECT 'row_level_projection', notEmpty(projections) = 0
FROM system.user_query_log
WHERE current_database=currentDatabase() AND type='QueryFinish' AND log_comment='t05239_row_level'
ORDER BY event_time_microseconds DESC LIMIT 1;

-- absolute: compare every grouping key, aggregate value, NULL flag and type.
INSERT INTO t05239_expected SELECT toString(arraySort(groupArray(tuple(cob,desk,trader,isNull(s),ifNull(s,0),toTypeName(s))))) FROM (SELECT cob,desk,trader,sum(pnl) s FROM t05239_merge  GROUP BY cob,desk,trader)
SETTINGS optimize_merge_neutral_sum_children=0, log_comment='t05239_absolute_off', optimize_merge_neutral_sum_children_max_rows=1;
SELECT 'absolute', (SELECT toString(arraySort(groupArray(tuple(cob,desk,trader,isNull(s),ifNull(s,0),toTypeName(s))))) FROM (SELECT cob,desk,trader,sum(pnl) s FROM t05239_merge  GROUP BY cob,desk,trader)) = (SELECT rows FROM t05239_expected)
SETTINGS optimize_merge_neutral_sum_children=1, log_comment='t05239_absolute', optimize_merge_neutral_sum_children_max_rows=1;
TRUNCATE TABLE t05239_expected;
SYSTEM FLUSH LOGS;
SELECT 'absolute_projection', notEmpty(projections) = 0
FROM system.user_query_log
WHERE current_database=currentDatabase() AND type='QueryFinish' AND log_comment='t05239_absolute'
ORDER BY event_time_microseconds DESC LIMIT 1;

-- ratio: compare every grouping key, aggregate value, NULL flag and type.
INSERT INTO t05239_expected SELECT toString(arraySort(groupArray(tuple(cob,desk,trader,isNull(s),ifNull(s,0),toTypeName(s))))) FROM (SELECT cob,desk,trader,sum(pnl) s FROM t05239_merge  GROUP BY cob,desk,trader)
SETTINGS optimize_merge_neutral_sum_children=0, log_comment='t05239_ratio_off', optimize_merge_neutral_sum_children_max_rows_ratio=0.000001;
SELECT 'ratio', (SELECT toString(arraySort(groupArray(tuple(cob,desk,trader,isNull(s),ifNull(s,0),toTypeName(s))))) FROM (SELECT cob,desk,trader,sum(pnl) s FROM t05239_merge  GROUP BY cob,desk,trader)) = (SELECT rows FROM t05239_expected)
SETTINGS optimize_merge_neutral_sum_children=1, log_comment='t05239_ratio', optimize_merge_neutral_sum_children_max_rows_ratio=0.000001;
TRUNCATE TABLE t05239_expected;
SYSTEM FLUSH LOGS;
SELECT 'ratio_projection', notEmpty(projections) = 0
FROM system.user_query_log
WHERE current_database=currentDatabase() AND type='QueryFinish' AND log_comment='t05239_ratio'
ORDER BY event_time_microseconds DESC LIMIT 1;

-- default: compare every grouping key, aggregate value, NULL flag and type.
INSERT INTO t05239_expected SELECT toString(arraySort(groupArray(tuple(cob,desk,trader,isNull(s),ifNull(s,0),toTypeName(s))))) FROM (SELECT cob,desk,trader,sum(pnl) s FROM t05239_default  GROUP BY cob,desk,trader)
SETTINGS optimize_merge_neutral_sum_children=0, log_comment='t05239_default_off';
SELECT 'default', (SELECT toString(arraySort(groupArray(tuple(cob,desk,trader,isNull(s),ifNull(s,0),toTypeName(s))))) FROM (SELECT cob,desk,trader,sum(pnl) s FROM t05239_default  GROUP BY cob,desk,trader)) = (SELECT rows FROM t05239_expected)
SETTINGS optimize_merge_neutral_sum_children=1, log_comment='t05239_default';
TRUNCATE TABLE t05239_expected;
SYSTEM FLUSH LOGS;
SELECT 'default_projection', notEmpty(projections) = 0
FROM system.user_query_log
WHERE current_database=currentDatabase() AND type='QueryFinish' AND log_comment='t05239_default'
ORDER BY event_time_microseconds DESC LIMIT 1;

-- avg: compare every grouping key, aggregate value, NULL flag and type.
INSERT INTO t05239_expected SELECT toString(arraySort(groupArray(tuple(cob,desk,trader,isNull(s),ifNull(s,0),toTypeName(s))))) FROM (SELECT cob,desk,trader,avg(pnl) s FROM t05239_merge  GROUP BY cob,desk,trader)
SETTINGS optimize_merge_neutral_sum_children=0, log_comment='t05239_avg_off';
SELECT 'avg', (SELECT toString(arraySort(groupArray(tuple(cob,desk,trader,isNull(s),ifNull(s,0),toTypeName(s))))) FROM (SELECT cob,desk,trader,avg(pnl) s FROM t05239_merge  GROUP BY cob,desk,trader)) = (SELECT rows FROM t05239_expected)
SETTINGS optimize_merge_neutral_sum_children=1, log_comment='t05239_avg';
TRUNCATE TABLE t05239_expected;
SYSTEM FLUSH LOGS;
SELECT 'avg_projection', notEmpty(projections) = 0
FROM system.user_query_log
WHERE current_database=currentDatabase() AND type='QueryFinish' AND log_comment='t05239_avg'
ORDER BY event_time_microseconds DESC LIMIT 1;

-- count: compare every grouping key, aggregate value, NULL flag and type.
INSERT INTO t05239_expected SELECT toString(arraySort(groupArray(tuple(cob,desk,trader,isNull(s),ifNull(s,0),toTypeName(s))))) FROM (SELECT cob,desk,trader,count() s FROM t05239_merge  GROUP BY cob,desk,trader)
SETTINGS optimize_merge_neutral_sum_children=0, log_comment='t05239_count_off';
SELECT 'count', (SELECT toString(arraySort(groupArray(tuple(cob,desk,trader,isNull(s),ifNull(s,0),toTypeName(s))))) FROM (SELECT cob,desk,trader,count() s FROM t05239_merge  GROUP BY cob,desk,trader)) = (SELECT rows FROM t05239_expected)
SETTINGS optimize_merge_neutral_sum_children=1, log_comment='t05239_count';
TRUNCATE TABLE t05239_expected;
SYSTEM FLUSH LOGS;
SELECT 'count_projection', notEmpty(projections) = 0
FROM system.user_query_log
WHERE current_database=currentDatabase() AND type='QueryFinish' AND log_comment='t05239_count'
ORDER BY event_time_microseconds DESC LIMIT 1;

-- final: compare every grouping key, aggregate value, NULL flag and type.
INSERT INTO t05239_expected SELECT toString(arraySort(groupArray(tuple(cob,desk,trader,isNull(s),ifNull(s,0),toTypeName(s))))) FROM (SELECT cob,desk,trader,sum(pnl) s FROM t05239_merge FINAL  GROUP BY cob,desk,trader)
SETTINGS optimize_merge_neutral_sum_children=0, log_comment='t05239_final_off';
SELECT 'final', (SELECT toString(arraySort(groupArray(tuple(cob,desk,trader,isNull(s),ifNull(s,0),toTypeName(s))))) FROM (SELECT cob,desk,trader,sum(pnl) s FROM t05239_merge FINAL  GROUP BY cob,desk,trader)) = (SELECT rows FROM t05239_expected)
SETTINGS optimize_merge_neutral_sum_children=1, log_comment='t05239_final';
TRUNCATE TABLE t05239_expected;
SYSTEM FLUSH LOGS;
SELECT 'final_projection', notEmpty(projections) = 0
FROM system.user_query_log
WHERE current_database=currentDatabase() AND type='QueryFinish' AND log_comment='t05239_final'
ORDER BY event_time_microseconds DESC LIMIT 1;
CREATE ROW POLICY t05239_policy ON t05239_keys USING desk = '0' TO default;

-- policy: compare every grouping key, aggregate value, NULL flag and type.
INSERT INTO t05239_expected SELECT toString(arraySort(groupArray(tuple(cob,desk,trader,isNull(s),ifNull(s,0),toTypeName(s))))) FROM (SELECT cob,desk,trader,sum(pnl) s FROM t05239_merge  GROUP BY cob,desk,trader)
SETTINGS optimize_merge_neutral_sum_children=0, log_comment='t05239_policy_off';
SELECT 'policy', (SELECT toString(arraySort(groupArray(tuple(cob,desk,trader,isNull(s),ifNull(s,0),toTypeName(s))))) FROM (SELECT cob,desk,trader,sum(pnl) s FROM t05239_merge  GROUP BY cob,desk,trader)) = (SELECT rows FROM t05239_expected)
SETTINGS optimize_merge_neutral_sum_children=1, log_comment='t05239_policy';
TRUNCATE TABLE t05239_expected;
SYSTEM FLUSH LOGS;
SELECT 'policy_projection', notEmpty(projections) = 0
FROM system.user_query_log
WHERE current_database=currentDatabase() AND type='QueryFinish' AND log_comment='t05239_policy'
ORDER BY event_time_microseconds DESC LIMIT 1;
DROP ROW POLICY t05239_policy ON t05239_keys;
CREATE VIEW t05239_view AS SELECT * FROM t05239_merge;

-- view: compare every grouping key, aggregate value, NULL flag and type.
INSERT INTO t05239_expected SELECT toString(arraySort(groupArray(tuple(cob,desk,trader,isNull(s),ifNull(s,0),toTypeName(s))))) FROM (SELECT cob,desk,trader,sum(pnl) s FROM t05239_view WHERE cob = '2026-09-08' GROUP BY cob,desk,trader)
SETTINGS optimize_merge_neutral_sum_children=0, log_comment='t05239_view_off';
SELECT 'view', (SELECT toString(arraySort(groupArray(tuple(cob,desk,trader,isNull(s),ifNull(s,0),toTypeName(s))))) FROM (SELECT cob,desk,trader,sum(pnl) s FROM t05239_view WHERE cob = '2026-09-08' GROUP BY cob,desk,trader)) = (SELECT rows FROM t05239_expected)
SETTINGS optimize_merge_neutral_sum_children=1, log_comment='t05239_view';
TRUNCATE TABLE t05239_expected;
SYSTEM FLUSH LOGS;
SELECT 'view_projection', notEmpty(projections) = 1
FROM system.user_query_log
WHERE current_database=currentDatabase() AND type='QueryFinish' AND log_comment='t05239_view'
ORDER BY event_time_microseconds DESC LIMIT 1;
SELECT 'view_reads', (SELECT read_rows
FROM system.user_query_log
WHERE current_database=currentDatabase() AND type='QueryFinish' AND log_comment='t05239_view'
ORDER BY event_time_microseconds DESC LIMIT 1) < (SELECT read_rows
FROM system.user_query_log
WHERE current_database=currentDatabase() AND type='QueryFinish' AND log_comment='t05239_view_off'
ORDER BY event_time_microseconds DESC LIMIT 1) / 10;
CREATE VIEW t05239_transformed AS SELECT cob, lower(desk) AS desk, concat(trader,'_x') AS trader, pnl FROM t05239_merge;

-- transformed: compare every grouping key, aggregate value, NULL flag and type.
INSERT INTO t05239_expected SELECT toString(arraySort(groupArray(tuple(cob,desk,trader,isNull(s),ifNull(s,0),toTypeName(s))))) FROM (SELECT cob,desk,trader,sum(pnl) s FROM t05239_transformed WHERE cob IN ('2026-09-08','2026-09-10') AND desk = '0' GROUP BY cob,desk,trader)
SETTINGS optimize_merge_neutral_sum_children=0, log_comment='t05239_transformed_off';
SELECT 'transformed', (SELECT toString(arraySort(groupArray(tuple(cob,desk,trader,isNull(s),ifNull(s,0),toTypeName(s))))) FROM (SELECT cob,desk,trader,sum(pnl) s FROM t05239_transformed WHERE cob IN ('2026-09-08','2026-09-10') AND desk = '0' GROUP BY cob,desk,trader)) = (SELECT rows FROM t05239_expected)
SETTINGS optimize_merge_neutral_sum_children=1, log_comment='t05239_transformed';
TRUNCATE TABLE t05239_expected;
SYSTEM FLUSH LOGS;
SELECT 'transformed_projection', notEmpty(projections) = 1
FROM system.user_query_log
WHERE current_database=currentDatabase() AND type='QueryFinish' AND log_comment='t05239_transformed'
ORDER BY event_time_microseconds DESC LIMIT 1;
SELECT 'transformed_reads', (SELECT read_rows
FROM system.user_query_log
WHERE current_database=currentDatabase() AND type='QueryFinish' AND log_comment='t05239_transformed'
ORDER BY event_time_microseconds DESC LIMIT 1) < (SELECT read_rows
FROM system.user_query_log
WHERE current_database=currentDatabase() AND type='QueryFinish' AND log_comment='t05239_transformed_off'
ORDER BY event_time_microseconds DESC LIMIT 1) / 10;

-- ordered: compare every grouping key, aggregate value, NULL flag and type.
INSERT INTO t05239_expected SELECT toString(arraySort(groupArray(tuple(cob,desk,trader,isNull(s),ifNull(s,0),toTypeName(s))))) FROM (SELECT cob,desk,trader,sum(pnl) s FROM t05239_view WHERE cob = '2026-09-08' GROUP BY cob,desk,trader)
SETTINGS optimize_merge_neutral_sum_children=0, log_comment='t05239_ordered_off', optimize_aggregation_in_order=1;
SELECT 'ordered', (SELECT toString(arraySort(groupArray(tuple(cob,desk,trader,isNull(s),ifNull(s,0),toTypeName(s))))) FROM (SELECT cob,desk,trader,sum(pnl) s FROM t05239_view WHERE cob = '2026-09-08' GROUP BY cob,desk,trader)) = (SELECT rows FROM t05239_expected)
SETTINGS optimize_merge_neutral_sum_children=1, log_comment='t05239_ordered', optimize_aggregation_in_order=1;
TRUNCATE TABLE t05239_expected;
SYSTEM FLUSH LOGS;
SELECT 'ordered_projection', notEmpty(projections) = 1
FROM system.user_query_log
WHERE current_database=currentDatabase() AND type='QueryFinish' AND log_comment='t05239_ordered'
ORDER BY event_time_microseconds DESC LIMIT 1;
SELECT 'ordered_reads', (SELECT read_rows
FROM system.user_query_log
WHERE current_database=currentDatabase() AND type='QueryFinish' AND log_comment='t05239_ordered'
ORDER BY event_time_microseconds DESC LIMIT 1) < (SELECT read_rows
FROM system.user_query_log
WHERE current_database=currentDatabase() AND type='QueryFinish' AND log_comment='t05239_ordered_off'
ORDER BY event_time_microseconds DESC LIMIT 1) / 10;

-- view_row_level: compare every grouping key, aggregate value, NULL flag and type.
INSERT INTO t05239_expected SELECT toString(arraySort(groupArray(tuple(cob,desk,trader,isNull(s),ifNull(s,0),toTypeName(s))))) FROM (SELECT cob,desk,trader,sum(pnl) s FROM t05239_view WHERE delta > 0 GROUP BY cob,desk,trader)
SETTINGS optimize_merge_neutral_sum_children=0, log_comment='t05239_view_row_level_off';
SELECT 'view_row_level', (SELECT toString(arraySort(groupArray(tuple(cob,desk,trader,isNull(s),ifNull(s,0),toTypeName(s))))) FROM (SELECT cob,desk,trader,sum(pnl) s FROM t05239_view WHERE delta > 0 GROUP BY cob,desk,trader)) = (SELECT rows FROM t05239_expected)
SETTINGS optimize_merge_neutral_sum_children=1, log_comment='t05239_view_row_level';
TRUNCATE TABLE t05239_expected;
SYSTEM FLUSH LOGS;
SELECT 'view_row_level_projection', notEmpty(projections) = 0
FROM system.user_query_log
WHERE current_database=currentDatabase() AND type='QueryFinish' AND log_comment='t05239_view_row_level'
ORDER BY event_time_microseconds DESC LIMIT 1;

-- view_measure: compare every grouping key, aggregate value, NULL flag and type.
INSERT INTO t05239_expected SELECT toString(arraySort(groupArray(tuple(cob,desk,trader,isNull(s),ifNull(s,0),toTypeName(s))))) FROM (SELECT cob,desk,trader,sum(pnl) s FROM t05239_view WHERE pnl > 0 GROUP BY cob,desk,trader)
SETTINGS optimize_merge_neutral_sum_children=0, log_comment='t05239_view_measure_off';
SELECT 'view_measure', (SELECT toString(arraySort(groupArray(tuple(cob,desk,trader,isNull(s),ifNull(s,0),toTypeName(s))))) FROM (SELECT cob,desk,trader,sum(pnl) s FROM t05239_view WHERE pnl > 0 GROUP BY cob,desk,trader)) = (SELECT rows FROM t05239_expected)
SETTINGS optimize_merge_neutral_sum_children=1, log_comment='t05239_view_measure';
TRUNCATE TABLE t05239_expected;
SYSTEM FLUSH LOGS;
SELECT 'view_measure_projection', notEmpty(projections) = 0
FROM system.user_query_log
WHERE current_database=currentDatabase() AND type='QueryFinish' AND log_comment='t05239_view_measure'
ORDER BY event_time_microseconds DESC LIMIT 1;
CREATE VIEW t05239_non_neutral AS SELECT cob,desk,trader,ifNull(pnl,7) AS pnl FROM t05239_merge;

-- view_computed_measure: compare every grouping key, aggregate value, NULL flag and type.
INSERT INTO t05239_expected SELECT toString(arraySort(groupArray(tuple(cob,desk,trader,isNull(s),ifNull(s,0),toTypeName(s))))) FROM (SELECT cob,desk,trader,sum(pnl) s FROM t05239_non_neutral  GROUP BY cob,desk,trader)
SETTINGS optimize_merge_neutral_sum_children=0, log_comment='t05239_view_computed_measure_off';
SELECT 'view_computed_measure', (SELECT toString(arraySort(groupArray(tuple(cob,desk,trader,isNull(s),ifNull(s,0),toTypeName(s))))) FROM (SELECT cob,desk,trader,sum(pnl) s FROM t05239_non_neutral  GROUP BY cob,desk,trader)) = (SELECT rows FROM t05239_expected)
SETTINGS optimize_merge_neutral_sum_children=1, log_comment='t05239_view_computed_measure';
TRUNCATE TABLE t05239_expected;
SYSTEM FLUSH LOGS;
SELECT 'view_computed_measure_projection', notEmpty(projections) = 0
FROM system.user_query_log
WHERE current_database=currentDatabase() AND type='QueryFinish' AND log_comment='t05239_view_computed_measure'
ORDER BY event_time_microseconds DESC LIMIT 1;
SYSTEM START MERGES t05239_keys;
ALTER TABLE t05239_keys DROP PROJECTION IF EXISTS p
SETTINGS mutations_sync=1;

-- missing: compare every grouping key, aggregate value, NULL flag and type.
INSERT INTO t05239_expected SELECT toString(arraySort(groupArray(tuple(cob,desk,trader,isNull(s),ifNull(s,0),toTypeName(s))))) FROM (SELECT cob,desk,trader,sum(pnl) s FROM t05239_merge  GROUP BY cob,desk,trader)
SETTINGS optimize_merge_neutral_sum_children=0, log_comment='t05239_missing_off';
SELECT 'missing', (SELECT toString(arraySort(groupArray(tuple(cob,desk,trader,isNull(s),ifNull(s,0),toTypeName(s))))) FROM (SELECT cob,desk,trader,sum(pnl) s FROM t05239_merge  GROUP BY cob,desk,trader)) = (SELECT rows FROM t05239_expected)
SETTINGS optimize_merge_neutral_sum_children=1, log_comment='t05239_missing';
TRUNCATE TABLE t05239_expected;
SYSTEM FLUSH LOGS;
SELECT 'missing_projection', notEmpty(projections) = 0
FROM system.user_query_log
WHERE current_database=currentDatabase() AND type='QueryFinish' AND log_comment='t05239_missing'
ORDER BY event_time_microseconds DESC LIMIT 1;
SYSTEM START MERGES t05239_keys;
ALTER TABLE t05239_keys DROP PROJECTION IF EXISTS p
SETTINGS mutations_sync=1;
ALTER TABLE t05239_keys ADD PROJECTION p (SELECT cob, desk, trader, sum(delta) GROUP BY cob, desk, trader);
SYSTEM STOP MERGES t05239_keys;
INSERT INTO t05239_keys SELECT toDate('2026-09-10'), 'new', 'new', 1 FROM numbers(100000);

-- partial: compare every grouping key, aggregate value, NULL flag and type.
INSERT INTO t05239_expected SELECT toString(arraySort(groupArray(tuple(cob,desk,trader,isNull(s),ifNull(s,0),toTypeName(s))))) FROM (SELECT cob,desk,trader,sum(pnl) s FROM t05239_merge  GROUP BY cob,desk,trader)
SETTINGS optimize_merge_neutral_sum_children=0, log_comment='t05239_partial_off';
SELECT 'partial', (SELECT toString(arraySort(groupArray(tuple(cob,desk,trader,isNull(s),ifNull(s,0),toTypeName(s))))) FROM (SELECT cob,desk,trader,sum(pnl) s FROM t05239_merge  GROUP BY cob,desk,trader)) = (SELECT rows FROM t05239_expected)
SETTINGS optimize_merge_neutral_sum_children=1, log_comment='t05239_partial';
TRUNCATE TABLE t05239_expected;
SYSTEM FLUSH LOGS;
SELECT 'partial_projection', notEmpty(projections) = 0
FROM system.user_query_log
WHERE current_database=currentDatabase() AND type='QueryFinish' AND log_comment='t05239_partial'
ORDER BY event_time_microseconds DESC LIMIT 1;
SYSTEM START MERGES t05239_keys;
ALTER TABLE t05239_keys DROP PROJECTION IF EXISTS p
SETTINGS mutations_sync=1;
ALTER TABLE t05239_keys ADD PROJECTION p (SELECT cob, desk, sum(delta) GROUP BY cob, desk);
SYSTEM START MERGES t05239_keys;
ALTER TABLE t05239_keys MATERIALIZE PROJECTION p
SETTINGS mutations_sync=1;

-- unsuitable: compare every grouping key, aggregate value, NULL flag and type.
INSERT INTO t05239_expected SELECT toString(arraySort(groupArray(tuple(cob,desk,trader,isNull(s),ifNull(s,0),toTypeName(s))))) FROM (SELECT cob,desk,trader,sum(pnl) s FROM t05239_merge  GROUP BY cob,desk,trader)
SETTINGS optimize_merge_neutral_sum_children=0, log_comment='t05239_unsuitable_off';
SELECT 'unsuitable', (SELECT toString(arraySort(groupArray(tuple(cob,desk,trader,isNull(s),ifNull(s,0),toTypeName(s))))) FROM (SELECT cob,desk,trader,sum(pnl) s FROM t05239_merge  GROUP BY cob,desk,trader)) = (SELECT rows FROM t05239_expected)
SETTINGS optimize_merge_neutral_sum_children=1, log_comment='t05239_unsuitable';
TRUNCATE TABLE t05239_expected;
SYSTEM FLUSH LOGS;
SELECT 'unsuitable_projection', notEmpty(projections) = 0
FROM system.user_query_log
WHERE current_database=currentDatabase() AND type='QueryFinish' AND log_comment='t05239_unsuitable'
ORDER BY event_time_microseconds DESC LIMIT 1;
SYSTEM START MERGES t05239_keys;
ALTER TABLE t05239_keys DROP PROJECTION IF EXISTS p
SETTINGS mutations_sync=1;
ALTER TABLE t05239_keys ADD PROJECTION p (SELECT cob, desk, trader, sum(delta) GROUP BY cob, desk, trader);
SYSTEM START MERGES t05239_keys;
ALTER TABLE t05239_keys MATERIALIZE PROJECTION p
SETTINGS mutations_sync=1;
ALTER TABLE t05239_keys ADD PROJECTION p2 (SELECT cob,desk,trader,delta,count() GROUP BY cob,desk,trader,delta);
ALTER TABLE t05239_keys MATERIALIZE PROJECTION p2
SETTINGS mutations_sync=1;

-- competing: compare every grouping key, aggregate value, NULL flag and type.
INSERT INTO t05239_expected SELECT toString(arraySort(groupArray(tuple(cob,desk,trader,isNull(s),ifNull(s,0),toTypeName(s))))) FROM (SELECT cob,desk,trader,sum(pnl) s FROM t05239_merge  GROUP BY cob,desk,trader)
SETTINGS optimize_merge_neutral_sum_children=0, log_comment='t05239_competing_off';
SELECT 'competing', (SELECT toString(arraySort(groupArray(tuple(cob,desk,trader,isNull(s),ifNull(s,0),toTypeName(s))))) FROM (SELECT cob,desk,trader,sum(pnl) s FROM t05239_merge  GROUP BY cob,desk,trader)) = (SELECT rows FROM t05239_expected)
SETTINGS optimize_merge_neutral_sum_children=1, log_comment='t05239_competing';
TRUNCATE TABLE t05239_expected;
SYSTEM FLUSH LOGS;
SELECT 'competing_projection', notEmpty(projections) = 1
FROM system.user_query_log
WHERE current_database=currentDatabase() AND type='QueryFinish' AND log_comment='t05239_competing'
ORDER BY event_time_microseconds DESC LIMIT 1;
SELECT 'competing_reads', (SELECT read_rows
FROM system.user_query_log
WHERE current_database=currentDatabase() AND type='QueryFinish' AND log_comment='t05239_competing'
ORDER BY event_time_microseconds DESC LIMIT 1) < (SELECT read_rows
FROM system.user_query_log
WHERE current_database=currentDatabase() AND type='QueryFinish' AND log_comment='t05239_competing_off'
ORDER BY event_time_microseconds DESC LIMIT 1) / 10;

-- A nondeterministic predicate must not be evaluated once per reduced group.
SELECT count() <= 34 FROM
(SELECT cob, desk, trader, sum(pnl) FROM t05239_merge WHERE rand64() % 2 = 0 GROUP BY cob, desk, trader)
SETTINGS optimize_merge_neutral_sum_children=1, log_comment='t05239_random';
SYSTEM FLUSH LOGS;
SELECT empty(projections)
FROM system.user_query_log
WHERE current_database=currentDatabase() AND type='QueryFinish' AND log_comment='t05239_random'
ORDER BY event_time_microseconds DESC LIMIT 1;
DROP VIEW t05239_non_neutral;
DROP VIEW t05239_transformed;
DROP VIEW t05239_view;
DROP TABLE t05239_expected;
DROP TABLE t05239_default;
DROP TABLE t05239_merge;
DROP TABLE t05239_keys;
DROP TABLE t05239_values;
