SET max_threads = 2;
SET log_queries = 1;
SET optimize_merge_neutral_sum_children = 1;
CREATE TABLE nm_values (k UInt64, pnl Nullable(Float64), id UInt64) ENGINE=MergeTree ORDER BY id SAMPLE BY id;
CREATE TABLE nm_keys (k UInt64, d Float64, id UInt64, PROJECTION p (SELECT k,sum(d) GROUP BY k))
ENGINE=MergeTree ORDER BY id SAMPLE BY id;
INSERT INTO nm_keys SELECT number % 2,1,number FROM numbers(100000);
CREATE TABLE nm_merge AS nm_values ENGINE=Merge(currentDatabase(), '^nm_(values|keys)$');
SYSTEM STOP MERGES nm_keys;
ALTER TABLE nm_keys UPDATE k=9 WHERE k=1 SETTINGS mutations_sync=0;
SELECT arraySort(groupArray(k)) FROM (SELECT k,sum(pnl) FROM nm_merge GROUP BY k)
SETTINGS apply_mutations_on_fly=1, log_comment='nm_pending';
SYSTEM FLUSH LOGS;
SELECT empty(projections) FROM system.user_query_log
WHERE current_database=currentDatabase() AND type='QueryFinish' AND log_comment='nm_pending'
ORDER BY event_time_microseconds DESC LIMIT 1;
SYSTEM START MERGES nm_keys;
ALTER TABLE nm_keys DELETE WHERE k=9 SETTINGS mutations_sync=1;
SELECT arraySort(groupArray(k)) FROM (SELECT k,sum(pnl) FROM nm_merge GROUP BY k);
ALTER TABLE nm_keys MODIFY SETTING lightweight_mutation_projection_mode='drop';
DELETE FROM nm_keys WHERE k=0 SETTINGS lightweight_deletes_sync=1;
SELECT count() FROM (SELECT k,sum(pnl) FROM nm_merge GROUP BY k);
INSERT INTO nm_keys SELECT number%2,1,number FROM numbers(100000);
SELECT count() FROM (SELECT k,sum(pnl) FROM nm_merge SAMPLE 0.5 GROUP BY k)
SETTINGS log_comment='nm_sample';
SYSTEM FLUSH LOGS;
SELECT empty(projections) FROM system.user_query_log
WHERE current_database=currentDatabase() AND type='QueryFinish' AND log_comment='nm_sample'
ORDER BY event_time_microseconds DESC LIMIT 1;
DROP TABLE nm_merge;
DROP TABLE nm_keys;
DROP TABLE nm_values;
