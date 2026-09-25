DROP TABLE IF EXISTS t05244_merge;
DROP TABLE IF EXISTS t05244_keys;
DROP TABLE IF EXISTS t05244_values;

SET max_threads=2;
-- Exercise rewrite semantics on small fixtures independently of the default cost gate.
SET optimize_merge_neutral_sum_children_min_read_bytes = 0;
SET optimize_merge_neutral_sum_children=1;
CREATE TABLE t05244_values (k Nullable(Int32), pnl Nullable(Float64)) ENGINE=MergeTree ORDER BY tuple();
CREATE TABLE t05244_keys (k Nullable(Int32), d Float64, PROJECTION p (SELECT k,sum(d) GROUP BY k)) ENGINE=MergeTree ORDER BY tuple();
INSERT INTO t05244_values VALUES (NULL,5),(1,7);
INSERT INTO t05244_keys SELECT if(number%2=0,NULL,2),1 FROM numbers(20000);
CREATE TABLE t05244_merge AS t05244_values ENGINE=Merge(currentDatabase(), '^t05244_(values|keys)$');
SELECT k,sum(pnl) FROM t05244_merge GROUP BY k ORDER BY k NULLS FIRST;
SELECT k,sum(pnl) FROM t05244_merge GROUP BY k ORDER BY k NULLS FIRST SETTINGS optimize_merge_neutral_sum_children=0;
SELECT k,sum(pnl),count() FROM t05244_merge GROUP BY k ORDER BY k NULLS FIRST;
SELECT k,sum(pnl) AS s FROM t05244_merge GROUP BY GROUPING SETS ((k),()) ORDER BY k NULLS FIRST,s;
SELECT k,sum(pnl) FROM (SELECT k,pnl,arrayJoin([1,2]) AS x FROM t05244_merge) GROUP BY k ORDER BY k NULLS FIRST;
DROP TABLE t05244_merge;
DROP TABLE t05244_keys;
DROP TABLE t05244_values;
