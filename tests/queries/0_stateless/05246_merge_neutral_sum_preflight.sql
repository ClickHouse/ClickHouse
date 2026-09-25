DROP TABLE IF EXISTS t05246_merge;
DROP TABLE IF EXISTS t05246_measure;
DROP TABLE IF EXISTS t05246_small;
DROP TABLE IF EXISTS t05246_large;
DROP TABLE IF EXISTS t05246_string_merge;
DROP TABLE IF EXISTS t05246_string_measure;
DROP TABLE IF EXISTS t05246_string_small;
DROP TABLE IF EXISTS t05246_string_large;
SET log_queries=1;
SET log_queries_min_type='QUERY_FINISH';
SET max_threads=2;
CREATE TABLE t05246_measure (k UInt64,pnl Nullable(Float64)) ENGINE=MergeTree ORDER BY k;
CREATE TABLE t05246_small (k UInt64,d UInt64,PROJECTION p (SELECT k,sum(d) GROUP BY k)) ENGINE=MergeTree ORDER BY k;
CREATE TABLE t05246_large AS t05246_small;
INSERT INTO t05246_measure VALUES (9,10);
INSERT INTO t05246_small SELECT number%10,1 FROM numbers(10000);
INSERT INTO t05246_large SELECT number%10+20,1 FROM numbers(1000000);
CREATE TABLE t05246_merge AS t05246_measure ENGINE=Merge(currentDatabase(), '^t05246_(measure|small|large)$');
-- Force a wide margin between compact small and wide large child metadata.
SELECT groupArray(tuple(k,isNull(s),ifNull(s,0))) FROM (SELECT k,sum(pnl) s FROM t05246_merge GROUP BY k ORDER BY k)
SETTINGS optimize_merge_neutral_sum_children=0;
SELECT arraySort(groupArray(tuple(k,isNull(s),ifNull(s,0)))) FROM
(SELECT k,sum(pnl) s FROM t05246_merge GROUP BY k)
SETTINGS optimize_merge_neutral_sum_children=1,optimize_merge_neutral_sum_children_min_read_bytes=3145728,log_comment='t05246_gate';
SYSTEM FLUSH LOGS;
SELECT arraySort(arrayMap(x -> substring(x,position(x,'.')+1),projections)) FROM system.user_query_log
WHERE current_database=currentDatabase() AND type='QueryFinish' AND log_comment='t05246_gate'
ORDER BY event_time_microseconds DESC LIMIT 1;
SELECT k,sum(pnl) FROM t05246_merge GROUP BY k ORDER BY k
SETTINGS optimize_merge_neutral_sum_children=1,optimize_merge_neutral_sum_children_min_read_bytes=0,log_comment='t05246_no_gate';
SYSTEM FLUSH LOGS;
SELECT arraySort(arrayMap(x -> substring(x,position(x,'.')+1),projections)) FROM system.user_query_log
WHERE current_database=currentDatabase() AND type='QueryFinish' AND log_comment='t05246_no_gate'
ORDER BY event_time_microseconds DESC LIMIT 1;
-- Stronger limit rejects both candidates before projection planning.
SELECT k,sum(pnl) FROM t05246_merge GROUP BY k ORDER BY k
SETTINGS optimize_merge_neutral_sum_children=1,optimize_merge_neutral_sum_children_min_read_bytes=1000000000,log_comment='t05246_all_raw';
SYSTEM FLUSH LOGS;
SELECT empty(projections) FROM system.user_query_log
WHERE current_database=currentDatabase() AND type='QueryFinish' AND log_comment='t05246_all_raw'
ORDER BY event_time_microseconds DESC LIMIT 1;
-- A large raw part with a selective key predicate must not pass the
-- whole-part-byte gate before the final selected marks are known.
SELECT k,sum(pnl) FROM t05246_merge WHERE k=20 GROUP BY k
SETTINGS optimize_merge_neutral_sum_children=1,optimize_merge_neutral_sum_children_min_read_bytes=3145728,log_comment='t05246_selective';
SYSTEM FLUSH LOGS;
SELECT empty(projections) FROM system.user_query_log
WHERE current_database=currentDatabase() AND type='QueryFinish' AND log_comment='t05246_selective'
ORDER BY event_time_microseconds DESC LIMIT 1;
-- Compact String parts have no per-column sizes. A small part can still be
-- rejected using total uncompressed column bytes as a whole-part upper bound.
CREATE TABLE t05246_string_measure (k String,pnl Nullable(Float64)) ENGINE=MergeTree ORDER BY k;
CREATE TABLE t05246_string_small (k String,d UInt64,PROJECTION p (SELECT k,sum(d) GROUP BY k)) ENGINE=MergeTree ORDER BY k;
CREATE TABLE t05246_string_large AS t05246_string_small;
INSERT INTO t05246_string_small SELECT toString(number%10),1 FROM numbers(10000);
INSERT INTO t05246_string_large SELECT toString(number%10+20),1 FROM numbers(300000);
CREATE TABLE t05246_string_merge AS t05246_string_measure
ENGINE=Merge(currentDatabase(), '^t05246_string_(measure|small|large)$');
SELECT arraySort(groupArray(tuple(k,isNull(s)))) FROM
(SELECT k,sum(pnl) s FROM t05246_string_merge GROUP BY k)
SETTINGS optimize_merge_neutral_sum_children=0;
SELECT arraySort(groupArray(tuple(k,isNull(s)))) FROM
(SELECT k,sum(pnl) s FROM t05246_string_merge GROUP BY k)
SETTINGS optimize_merge_neutral_sum_children=1,optimize_merge_neutral_sum_children_min_read_bytes=3145728,
log_comment='t05246_string_gate';
SYSTEM FLUSH LOGS;
SELECT arraySort(arrayMap(x -> substring(x,position(x,'.')+1),projections)) FROM system.user_query_log
WHERE current_database=currentDatabase() AND type='QueryFinish' AND log_comment='t05246_string_gate'
ORDER BY event_time_microseconds DESC LIMIT 1;
DROP TABLE t05246_string_merge;
DROP TABLE t05246_string_large;
DROP TABLE t05246_string_small;
DROP TABLE t05246_string_measure;
DROP TABLE t05246_merge;
DROP TABLE t05246_large;
DROP TABLE t05246_small;
DROP TABLE t05246_measure;
