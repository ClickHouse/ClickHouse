DROP VIEW IF EXISTS t05261_view;
DROP TABLE IF EXISTS t05261_merge;
DROP TABLE IF EXISTS t05261_right;
DROP TABLE IF EXISTS t05261_left;
DROP TABLE IF EXISTS t05261_values;

SET max_threads = 2;
-- Exercise rewrite semantics on small fixtures independently of the default cost gate.
SET optimize_merge_neutral_sum_children_min_read_bytes = 0;
SET log_queries = 1;
SET log_queries_min_type = 'QUERY_FINISH';
SET optimize_merge_neutral_sum_children = 1;
CREATE TABLE t05261_values (k String, pnl Nullable(Decimal(18,2))) ENGINE=MergeTree ORDER BY k;
CREATE TABLE t05261_left (k String, d UInt64, PROJECTION p (SELECT k,sum(d) GROUP BY k)) ENGINE=MergeTree ORDER BY k;
CREATE TABLE t05261_right AS t05261_left;
INSERT INTO t05261_values VALUES ('A',10),('a',20),('C',NULL);
INSERT INTO t05261_left SELECT if(number%2=0,'A','B'),1 FROM numbers(20000);
INSERT INTO t05261_right SELECT if(number%2=0,'a','D'),1 FROM numbers(20000);
CREATE TABLE t05261_merge AS t05261_values ENGINE=Merge(currentDatabase(), '^t05261_(values|left|right)$');
CREATE VIEW t05261_view AS SELECT lower(k) AS k,pnl FROM t05261_merge;
SELECT countIf(match(explain, 'AggregatingTransform × ([2-9]|[1-9][0-9]+)([^0-9]|$)')) > 0
FROM (EXPLAIN PIPELINE SELECT k,sum(pnl) FROM t05261_merge GROUP BY k);
SELECT k,sum(pnl) FROM t05261_merge GROUP BY k ORDER BY k
SETTINGS log_comment='05261_direct';
SELECT k,sum(pnl) FROM (SELECT * FROM t05261_merge) GROUP BY k ORDER BY k
SETTINGS log_comment='05261_subquery';
SELECT k,sum(pnl) FROM t05261_view GROUP BY k ORDER BY k
SETTINGS log_comment='05261_collapse';
SELECT k,sum(pnl) FROM t05261_view GROUP BY k ORDER BY k
SETTINGS optimize_merge_neutral_sum_children=0;
SELECT k,sum(pnl) FROM t05261_merge GROUP BY k ORDER BY k LIMIT 2
SETTINGS log_comment='05261_limit';
SELECT k,s FROM (SELECT k,sum(pnl) s FROM t05261_view GROUP BY k) WHERE isNull(s) ORDER BY k
SETTINGS log_comment='05261_having';
SYSTEM FLUSH LOGS;
SELECT log_comment,
    arraySort(arrayMap(x -> substring(x, position(x,'.')+1), projections)),
    read_rows < 100
FROM system.user_query_log
WHERE current_database=currentDatabase() AND type='QueryFinish'
AND log_comment IN ('05261_direct','05261_subquery','05261_collapse','05261_limit','05261_having')
ORDER BY log_comment;
SELECT k,sum(pnl) FROM t05261_merge GROUP BY k ORDER BY k LIMIT 1
SETTINGS query_plan_enable_optimizations=0,log_comment='05261_disabled';
SELECT k,sum(pnl) FROM t05261_merge GROUP BY k ORDER BY k LIMIT 1
SETTINGS optimize_use_projections=0,log_comment='05261_no_projections';
SYSTEM FLUSH LOGS;
SELECT log_comment,empty(projections) FROM system.user_query_log
WHERE current_database=currentDatabase() AND type='QueryFinish'
AND log_comment IN ('05261_disabled','05261_no_projections') ORDER BY log_comment;
DROP VIEW t05261_view;
DROP TABLE t05261_merge;
DROP TABLE t05261_right;
DROP TABLE t05261_left;
DROP TABLE t05261_values;
