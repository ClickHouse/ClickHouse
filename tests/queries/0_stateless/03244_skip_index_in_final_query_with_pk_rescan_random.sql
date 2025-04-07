-- Tags: long, no-random-settings, no-tsan, no-asan, no-msan, no-s3-storage
SELECT 'Starting the test';

DROP TABLE IF EXISTS st;
CREATE TABLE st (id Int32, v Int32, r Int32, INDEX bfv v TYPE bloom_filter) ENGINE=ReplacingMergeTree ORDER BY (id) SETTINGS index_granularity=64;
SYSTEM STOP MERGES st;

INSERT INTO st SELECT id % 9999999, if(id % 729 = 0, 4, v), 1  FROM (SELECT * FROM generateRandom('id UInt32, v UInt32', toUnixTimestamp(now())) limit 1000000) SETTINGS max_threads=1;
INSERT INTO st SELECT id % 9999999, if(id % 243 = 0, 3, v), 2  FROM (SELECT * FROM generateRandom('id UInt32, v UInt32', toUnixTimestamp(now()) + 1) limit 1000000) SETTINGS max_threads=1;
INSERT INTO st SELECT id % 9999999, if(id % 81 = 0, 2, v), 3  FROM (SELECT * FROM generateRandom('id UInt32, v UInt32', toUnixTimestamp(now()) + 2) limit 1000000) SETTINGS max_threads=1;
INSERT into st SELECT id % 9999999, if(id % 27 = 0, 1, v), 4  FROM (SELECT * FROM generateRandom('id UInt32, v UInt32', toUnixTimestamp(now()) + 3) limit 1000000) SETTINGS max_threads=1;

SELECT (id) FROM st FINAL WHERE v = 1 SETTINGS use_skip_indexes_if_final=0,use_skip_indexes_if_final_exact_mode=0
EXCEPT
SELECT (id) FROM st FINAL WHERE v = 1 SETTINGS use_skip_indexes_if_final=1,use_skip_indexes_if_final_exact_mode=1;

SELECT (id) FROM st FINAL WHERE v = 2 SETTINGS use_skip_indexes_if_final=0,use_skip_indexes_if_final_exact_mode=0
EXCEPT
SELECT (id) FROM st FINAL WHERE v = 2 SETTINGS use_skip_indexes_if_final=1,use_skip_indexes_if_final_exact_mode=1;

SELECT COUNT(id) FROM st FINAL WHERE v = 3 SETTINGS use_skip_indexes_if_final=0,use_skip_indexes_if_final_exact_mode=0
EXCEPT
SELECT COUNT(id) FROM st FINAL WHERE v = 3 SETTINGS use_skip_indexes_if_final=1,use_skip_indexes_if_final_exact_mode=1;

SELECT COUNT(id) FROM st FINAL WHERE v = 4 SETTINGS use_skip_indexes_if_final=0,use_skip_indexes_if_final_exact_mode=0
EXCEPT
SELECT COUNT(id) FROM st FINAL WHERE v = 4 SETTINGS use_skip_indexes_if_final=1,use_skip_indexes_if_final_exact_mode=1;

SELECT 'Test completed, there should no output above.';
