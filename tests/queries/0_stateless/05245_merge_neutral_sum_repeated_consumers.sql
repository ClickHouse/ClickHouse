DROP VIEW IF EXISTS t05245_view;
DROP TABLE IF EXISTS t05245_merge;
DROP TABLE IF EXISTS t05245_keys;
DROP TABLE IF EXISTS t05245_values;
SET max_threads=2;
SET optimize_merge_neutral_sum_children=1;
SET enable_materialized_cte=1;
CREATE TABLE t05245_values (k UInt64,pnl Nullable(Float64)) ENGINE=MergeTree ORDER BY k;
CREATE TABLE t05245_keys (k UInt64,d UInt64,PROJECTION p (SELECT k,sum(d) GROUP BY k)) ENGINE=MergeTree ORDER BY k;
INSERT INTO t05245_values VALUES (0,10),(0,20);
INSERT INTO t05245_keys SELECT number%2,1 FROM numbers(10000);
CREATE TABLE t05245_merge AS t05245_values ENGINE=Merge(currentDatabase(), '^t05245_(values|keys)$');
CREATE VIEW t05245_view AS SELECT k,pnl FROM t05245_merge;
-- A common materialized source also feeds a multiplicity-sensitive consumer.
WITH shared AS MATERIALIZED (SELECT * FROM t05245_view)
SELECT (SELECT count() FROM shared),
       (SELECT sum(s) FROM (SELECT k,sum(pnl) s FROM shared GROUP BY k));
WITH shared AS MATERIALIZED (SELECT * FROM t05245_view)
SELECT (SELECT count() FROM shared),
       (SELECT sum(s) FROM (SELECT k,sum(pnl) s FROM shared GROUP BY k))
SETTINGS optimize_merge_neutral_sum_children=0;
-- Reusing the view from independent aggregations must not share a proof for
-- different grouping-key expressions or leak reduction to a count consumer.
SELECT
 (SELECT sum(s) FROM (SELECT k,sum(pnl) s FROM t05245_view GROUP BY k)),
 (SELECT sum(s) FROM (SELECT k%2 AS bucket,sum(pnl) s FROM t05245_view GROUP BY bucket)),
 (SELECT count() FROM t05245_view);
SELECT
 (SELECT sum(s) FROM (SELECT k,sum(pnl) s FROM t05245_view GROUP BY k)),
 (SELECT sum(s) FROM (SELECT k%2 AS bucket,sum(pnl) s FROM t05245_view GROUP BY bucket)),
 (SELECT count() FROM t05245_view)
SETTINGS optimize_merge_neutral_sum_children=0;
DROP VIEW t05245_view;
DROP TABLE t05245_merge;
DROP TABLE t05245_keys;
DROP TABLE t05245_values;
