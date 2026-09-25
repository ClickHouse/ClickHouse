-- Tags: no-parallel
-- The failpoint injects a stateful filter after child creation and is server-global.
DROP VIEW IF EXISTS t05240_view;
DROP TABLE IF EXISTS t05240_merge;
DROP TABLE IF EXISTS t05240_keys_other;
DROP TABLE IF EXISTS t05240_keys;
DROP TABLE IF EXISTS t05240_values;

SET max_threads = 1;
SET log_queries = 1;
SET optimize_merge_neutral_sum_children = 1;
CREATE TABLE t05240_values (k UInt64, pnl Nullable(Float64)) ENGINE=MergeTree ORDER BY k;
CREATE TABLE t05240_keys (k UInt64, d Float64, PROJECTION p (SELECT k,sum(d) GROUP BY k)) ENGINE=MergeTree ORDER BY k;
INSERT INTO t05240_keys SELECT 7,1 FROM numbers(100000);
CREATE TABLE t05240_keys_other AS t05240_keys;
INSERT INTO t05240_keys_other SELECT 8,1 FROM numbers(100000);
INSERT INTO t05240_values VALUES (9,10),(9,20);
CREATE TABLE t05240_merge AS t05240_values ENGINE=Merge(currentDatabase(), '^t05240_(values|keys|keys_other)$');
CREATE VIEW t05240_view AS SELECT * FROM t05240_merge;
SELECT k,sum(pnl) FROM t05240_view GROUP BY k ORDER BY k SETTINGS log_comment='t05240_before';
SYSTEM FLUSH LOGS;
SELECT notEmpty(projections), read_rows < 100 FROM system.user_query_log
WHERE current_database=currentDatabase() AND type='QueryFinish' AND log_comment='t05240_before'
ORDER BY event_time_microseconds DESC LIMIT 1;
SYSTEM ENABLE FAILPOINT merge_neutral_sum_late_filter;
SELECT k, if(k=9, s BETWEEN 10 AND 30, isNull(s)) FROM
(SELECT k,sum(pnl) AS s FROM t05240_view GROUP BY k) ORDER BY k SETTINGS log_comment='t05240_after';
SYSTEM DISABLE FAILPOINT merge_neutral_sum_late_filter;
SYSTEM FLUSH LOGS;
SELECT empty(projections), read_rows >= 200000 FROM system.user_query_log
WHERE current_database=currentDatabase() AND type='QueryFinish' AND log_comment='t05240_after'
ORDER BY event_time_microseconds DESC LIMIT 1;
DROP VIEW t05240_view;
DROP TABLE t05240_merge;
DROP TABLE t05240_keys_other;
DROP TABLE t05240_keys;
DROP TABLE t05240_values;
