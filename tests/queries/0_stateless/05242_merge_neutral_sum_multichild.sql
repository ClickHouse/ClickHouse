DROP VIEW IF EXISTS t05242_view;
DROP TABLE IF EXISTS t05242_merge;
DROP TABLE IF EXISTS t05242_right;
DROP TABLE IF EXISTS t05242_left;
DROP TABLE IF EXISTS t05242_values;

SET max_threads = 2;
-- Exercise rewrite semantics on small fixtures independently of the default cost gate.
SET optimize_merge_neutral_sum_children_min_read_bytes = 0;
SET log_queries = 1;
SET log_queries_min_type = 'QUERY_FINISH';
SET optimize_merge_neutral_sum_children = 1;
CREATE TABLE t05242_values (k String, pnl Nullable(Decimal(18,2))) ENGINE=MergeTree ORDER BY k;
CREATE TABLE t05242_left (k String, d UInt64, PROJECTION p (SELECT k,sum(d) GROUP BY k)) ENGINE=MergeTree ORDER BY k;
CREATE TABLE t05242_right AS t05242_left;
INSERT INTO t05242_values VALUES ('A',10),('a',20),('C',NULL);
INSERT INTO t05242_left SELECT if(number%2=0,'A','B'),1 FROM numbers(20000);
INSERT INTO t05242_right SELECT if(number%2=0,'a','D'),1 FROM numbers(20000);
CREATE TABLE t05242_merge AS t05242_values ENGINE=Merge(currentDatabase(), '^t05242_(values|left|right)$');
CREATE VIEW t05242_view AS SELECT lower(k) AS k,pnl FROM t05242_merge;
SELECT countIf(match(explain, 'AggregatingTransform × ([2-9]|[1-9][0-9]+)([^0-9]|$)')) > 0
FROM (EXPLAIN PIPELINE SELECT k,sum(pnl) FROM t05242_merge GROUP BY k);
SELECT k,sum(pnl) FROM t05242_merge GROUP BY k ORDER BY k
SETTINGS log_comment='05242_direct';
SELECT k,sum(pnl) FROM (SELECT * FROM t05242_merge) GROUP BY k ORDER BY k
SETTINGS log_comment='05242_subquery';
SELECT k,sum(pnl) FROM t05242_view GROUP BY k ORDER BY k
SETTINGS log_comment='05242_collapse';
SELECT k,sum(pnl) FROM t05242_view GROUP BY k ORDER BY k
SETTINGS optimize_merge_neutral_sum_children=0;
SELECT k,sum(pnl) FROM t05242_merge GROUP BY k ORDER BY k LIMIT 2
SETTINGS log_comment='05242_limit';
SELECT k,s FROM (SELECT k,sum(pnl) s FROM t05242_view GROUP BY k) WHERE isNull(s) ORDER BY k
SETTINGS log_comment='05242_having';
SYSTEM FLUSH LOGS;
SELECT log_comment,
    arraySort(arrayMap(x -> substring(x, position(x,'.')+1), projections)),
    read_rows < 100
FROM system.user_query_log
WHERE current_database=currentDatabase() AND type='QueryFinish'
AND log_comment IN ('05242_direct','05242_subquery','05242_collapse','05242_limit','05242_having')
ORDER BY log_comment;
SELECT k,sum(pnl) FROM t05242_merge GROUP BY k ORDER BY k LIMIT 1
SETTINGS query_plan_enable_optimizations=0,log_comment='05242_disabled';
SELECT k,sum(pnl) FROM t05242_merge GROUP BY k ORDER BY k LIMIT 1
SETTINGS optimize_use_projections=0,log_comment='05242_no_projections';
SYSTEM FLUSH LOGS;
SELECT log_comment,empty(projections) FROM system.user_query_log
WHERE current_database=currentDatabase() AND type='QueryFinish'
AND log_comment IN ('05242_disabled','05242_no_projections') ORDER BY log_comment;
DROP VIEW t05242_view;
DROP TABLE t05242_merge;
DROP TABLE t05242_right;
DROP TABLE t05242_left;
DROP TABLE t05242_values;
