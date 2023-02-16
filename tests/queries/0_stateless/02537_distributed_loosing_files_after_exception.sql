/*
Testing that distributed table doesn't loose file after inserts which contain errors
*/

-- with monitor_batch_insert=1
DROP TABLE IF EXISTS dist;
DROP TABLE IF EXISTS underlying;

CREATE TABLE dist (key Int) ENGINE=Distributed(test_shard_localhost, currentDatabase(), underlying) SETTINGS monitor_batch_inserts=1;
SYSTEM STOP DISTRIBUTED SENDS dist;
INSERT INTO dist SETTINGS prefer_localhost_replica=0, max_threads=1 VALUES (1);
INSERT INTO dist SETTINGS prefer_localhost_replica=0, max_threads=2 VALUES (1);
SYSTEM FLUSH DISTRIBUTED dist; -- { serverError UNKNOWN_TABLE }
-- check the second since after using queue it may got lost from it
SYSTEM FLUSH DISTRIBUTED dist; -- { serverError UNKNOWN_TABLE }

SELECT is_blocked, data_files FROM system.distribution_queue WHERE database = currentDatabase() AND table = 'dist';

CREATE TABLE underlying (key Int) ENGINE=Memory();
SYSTEM FLUSH DISTRIBUTED dist;

-- all data should be flushed
SELECT is_blocked, data_files FROM system.distribution_queue WHERE database = currentDatabase() AND table = 'dist';

-- 2 2
SELECT sum(key), count(key) FROM dist;
SELECT sum(key), count(key) FROM underlying;

/*
Testing that we do not loose files with monitor_batch_insert=0
*/
DROP TABLE IF EXISTS dist;
DROP TABLE IF EXISTS underlying;

CREATE TABLE dist (key Int) ENGINE=Distributed(test_shard_localhost, currentDatabase(), underlying) SETTINGS monitor_batch_inserts=0;
SYSTEM STOP DISTRIBUTED SENDS dist;
INSERT INTO dist SETTINGS prefer_localhost_replica=0, max_threads=1 VALUES (1);
INSERT INTO dist SETTINGS prefer_localhost_replica=0, max_threads=2 VALUES (1);
SYSTEM FLUSH DISTRIBUTED dist; -- { serverError UNKNOWN_TABLE }
-- check the second since after using queue it may got lost from it
SYSTEM FLUSH DISTRIBUTED dist; -- { serverError UNKNOWN_TABLE }

SELECT is_blocked, data_files FROM system.distribution_queue WHERE database = currentDatabase() AND table = 'dist';

CREATE TABLE underlying (key Int) ENGINE=Memory();
SYSTEM FLUSH DISTRIBUTED dist;

-- distribution_queue should be empty
SELECT is_blocked, data_files FROM system.distribution_queue WHERE database = currentDatabase() AND table = 'dist';

-- 2 2
SELECT sum(key), count(key) FROM dist;
SELECT sum(key), count(key) FROM underlying;

/*
Testing that start up with current_batch.txt works
*/
DROP TABLE IF EXISTS dist;
DROP TABLE IF EXISTS underlying;
CREATE TABLE dist (key Int) ENGINE=Distributed(test_shard_localhost, currentDatabase(), underlying) SETTINGS monitor_batch_inserts=1;
SYSTEM STOP DISTRIBUTED SENDS dist;
INSERT INTO dist SETTINGS prefer_localhost_replica=0, max_threads=1 VALUES (1); 
INSERT INTO dist SETTINGS prefer_localhost_replica=0, max_threads=2 VALUES (1);
SYSTEM FLUSH DISTRIBUTED dist; -- { serverError UNKNOWN_TABLE }
-- check the second since after using queue it may got lost from it
SYSTEM FLUSH DISTRIBUTED dist; -- { serverError UNKNOWN_TABLE }

-- 2 inserts should be blocked
SELECT is_blocked, data_files FROM system.distribution_queue WHERE database = currentDatabase() AND table = 'dist';

DETACH TABLE dist;
CREATE TABLE underlying (key Int) ENGINE=Memory();
ATTACH TABLE dist;

SYSTEM FLUSH DISTRIBUTED dist;

-- distribution queue should be empty
SELECT is_blocked, data_files FROM system.distribution_queue WHERE database = currentDatabase() AND table = 'dist';

-- 2 2
SELECT sum(key), count(key) FROM dist;
SELECT sum(key), count(key) FROM underlying;
