DROP TABLE IF EXISTS t_03263;

CREATE TABLE t_03263(k UInt64) ENGINE = MergeTree() ORDER BY k;

-- Create table with 6 parts
SYSTEM STOP MERGES t_03263;

INSERT INTO t_03263 VALUES(1);
INSERT INTO t_03263 VALUES(2);
INSERT INTO t_03263 VALUES(3);
INSERT INTO t_03263 VALUES(4);
INSERT INTO t_03263 VALUES(5);
INSERT INTO t_03263 VALUES(6);

SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 't_03263';

SELECT * FROM t_03263 LIMIT 1 SETTINGS max_threads=1, max_threads_for_indexes=0, log_comment='t_03263--1' FORMAT Null;
SELECT * FROM t_03263 LIMIT 1 SETTINGS max_threads=1, max_threads_for_indexes=4, log_comment='t_03263--2' FORMAT Null;
SELECT * FROM t_03263 LIMIT 1 SETTINGS max_threads=4, max_threads_for_indexes=1, log_comment='t_03263--3' FORMAT Null;
SELECT * FROM t_03263 LIMIT 1 SETTINGS max_threads=4, max_threads_for_indexes=4, log_comment='t_03263--4' FORMAT Null;

SYSTEM FLUSH LOGS;

-- Check if queries where using local thread pool (with max_threads_for_indexes > 1 they should)
SELECT log_comment, ProfileEvents['SelectedPartsTotal'],  ProfileEvents['LocalThreadPoolJobs'] >= 6 FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment LIKE 't_03263--%'
ORDER BY log_comment;
