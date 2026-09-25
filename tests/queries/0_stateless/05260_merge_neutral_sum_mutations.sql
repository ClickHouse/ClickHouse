DROP TABLE IF EXISTS t05260_merge;
DROP TABLE IF EXISTS t05260_keys;
DROP TABLE IF EXISTS t05260_values;

SET max_threads = 2;
-- Exercise rewrite semantics on small fixtures independently of the default cost gate.
SET optimize_merge_neutral_sum_children_min_read_bytes = 0;
SET log_queries = 1;
SET optimize_merge_neutral_sum_children = 1;
CREATE TABLE t05260_values (k UInt64, pnl Nullable(Float64), id UInt64) ENGINE=MergeTree ORDER BY id SAMPLE BY id;
CREATE TABLE t05260_keys (k UInt64, d Float64, id UInt64, PROJECTION p (SELECT k,sum(d) GROUP BY k))
ENGINE=MergeTree ORDER BY id SAMPLE BY id;
INSERT INTO t05260_keys SELECT number % 2,1,number FROM numbers(100000);
CREATE TABLE t05260_merge AS t05260_values ENGINE=Merge(currentDatabase(), '^t05260_(values|keys)$');
SYSTEM STOP MERGES t05260_keys;
ALTER TABLE t05260_keys UPDATE k=9 WHERE k=1 SETTINGS mutations_sync=0;
SELECT arraySort(groupArray(k)) FROM (SELECT k,sum(pnl) FROM t05260_merge GROUP BY k)
SETTINGS apply_mutations_on_fly=1, log_comment='t05260_pending';
SYSTEM FLUSH LOGS;
SELECT empty(projections) FROM system.user_query_log
WHERE current_database=currentDatabase() AND type='QueryFinish' AND log_comment='t05260_pending'
ORDER BY event_time_microseconds DESC LIMIT 1;
SYSTEM START MERGES t05260_keys;
ALTER TABLE t05260_keys DELETE WHERE k=9 SETTINGS mutations_sync=1;
SELECT arraySort(groupArray(k)) FROM (SELECT k,sum(pnl) FROM t05260_merge GROUP BY k);
ALTER TABLE t05260_keys MODIFY SETTING lightweight_mutation_projection_mode='drop';
DELETE FROM t05260_keys WHERE k=0 SETTINGS lightweight_deletes_sync=1;
SELECT count() FROM (SELECT k,sum(pnl) FROM t05260_merge GROUP BY k);
INSERT INTO t05260_keys SELECT number%2,1,number FROM numbers(100000);
SELECT count() FROM (SELECT k,sum(pnl) FROM t05260_merge SAMPLE 0.5 GROUP BY k)
SETTINGS log_comment='t05260_sample';
SYSTEM FLUSH LOGS;
SELECT empty(projections) FROM system.user_query_log
WHERE current_database=currentDatabase() AND type='QueryFinish' AND log_comment='t05260_sample'
ORDER BY event_time_microseconds DESC LIMIT 1;
DROP TABLE t05260_merge;
DROP TABLE t05260_keys;
DROP TABLE t05260_values;
