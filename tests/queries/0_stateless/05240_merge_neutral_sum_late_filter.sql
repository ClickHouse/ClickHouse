-- Tags: no-parallel
-- The failpoint injects a stateful filter after child creation and is server-global.
SET max_threads = 1;
SET log_queries = 1;
SET optimize_merge_neutral_sum_children = 1;
CREATE TABLE nl_values (k UInt64, pnl Nullable(Float64)) ENGINE=MergeTree ORDER BY k;
CREATE TABLE nl_keys (k UInt64, d Float64, PROJECTION p (SELECT k,sum(d) GROUP BY k)) ENGINE=MergeTree ORDER BY k;
INSERT INTO nl_keys SELECT 7,1 FROM numbers(100000);
CREATE TABLE nl_merge AS nl_values ENGINE=Merge(currentDatabase(), '^nl_(values|keys)$');
SELECT k,sum(pnl) FROM nl_merge GROUP BY k SETTINGS log_comment='nl_before';
SYSTEM FLUSH LOGS;
SELECT notEmpty(projections), read_rows < 100 FROM system.user_query_log
WHERE current_database=currentDatabase() AND type='QueryFinish' AND log_comment='nl_before'
ORDER BY event_time_microseconds DESC LIMIT 1;
SYSTEM ENABLE FAILPOINT merge_neutral_sum_late_filter;
SELECT k,sum(pnl) FROM nl_merge GROUP BY k SETTINGS log_comment='nl_after';
SYSTEM DISABLE FAILPOINT merge_neutral_sum_late_filter;
SYSTEM FLUSH LOGS;
SELECT empty(projections), read_rows >= 100000 FROM system.user_query_log
WHERE current_database=currentDatabase() AND type='QueryFinish' AND log_comment='nl_after'
ORDER BY event_time_microseconds DESC LIMIT 1;
DROP TABLE nl_merge;
DROP TABLE nl_keys;
DROP TABLE nl_values;
