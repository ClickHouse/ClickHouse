DROP TABLE IF EXISTS test.merge_tree_table;

CREATE TABLE test.merge_tree_table (id UInt64, date Date, uid UInt32) ENGINE = MergeTree(date, id, 8192);

INSERT INTO test.merge_tree_table SELECT (intHash64(number)) % 10000, toDate('2018-08-01'), rand() FROM system.numbers LIMIT 10000000;

OPTIMIZE TABLE test.merge_tree_table FINAL;

SELECT count() from (SELECT toDayOfWeek(date) as m, id, count() FROM test.merge_tree_table GROUP BY id, m ORDER BY count() DESC LIMIT 10 SETTINGS max_threads = 1, log_queries=1, log_query_threads=1, log_profile_events=1, log_query_settings=1); -- output doesn't matter

-- sleep for sure
SELECT sleep(3);
SELECT sleep(3);
SELECT sleep(3);

SELECT pi.Values FROM system.query_log ARRAY JOIN ProfileEvents as pi WHERE query='SELECT count() from (SELECT toDayOfWeek(date) as m, id, count() FROM test.merge_tree_table GROUP BY id, m ORDER BY count() DESC LIMIT 10 SETTINGS max_threads = 1, log_queries=1, log_query_threads=1, log_profile_events=1, log_query_settings=1)' and pi.Names = 'FileOpen' ORDER BY event_time DESC LIMIT 1;

DROP TABLE IF EXISTS test.merge_tree_table;
