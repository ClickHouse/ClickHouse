SET distributed_directory_monitor_batch_inserts=1;
SET distributed_directory_monitor_sleep_time_ms=10;
SET distributed_directory_monitor_max_sleep_time_ms=100;

DROP TABLE IF EXISTS test_01040;
DROP TABLE IF EXISTS dist_test_01040;

CREATE TABLE test_01040 (key UInt64) ENGINE=TinyLog();
CREATE TABLE dist_test_01040 AS test_01040 Engine=Distributed(test_cluster_two_shards, currentDatabase(), test_01040, key);

-- internal_replication=false
SELECT 'test_cluster_two_shards prefer_localhost_replica=0';
SET prefer_localhost_replica=0;
INSERT INTO dist_test_01040 SELECT toUInt64(number) FROM numbers(2);
SYSTEM FLUSH DISTRIBUTED dist_test_01040;
SELECT * FROM dist_test_01040 ORDER BY key;
TRUNCATE TABLE test_01040;

SELECT 'test_cluster_two_shards prefer_localhost_replica=1';
SET prefer_localhost_replica=1;
INSERT INTO dist_test_01040 SELECT toUInt64(number) FROM numbers(2);
SYSTEM FLUSH DISTRIBUTED dist_test_01040;
SELECT * FROM dist_test_01040 ORDER BY key;
TRUNCATE TABLE test_01040;

DROP TABLE dist_test_01040;

-- internal_replication=true
CREATE TABLE dist_test_01040 AS test_01040 Engine=Distributed(test_cluster_two_shards_internal_replication, currentDatabase(), test_01040, key);
SELECT 'test_cluster_two_shards_internal_replication prefer_localhost_replica=0';
SET prefer_localhost_replica=0;
INSERT INTO dist_test_01040 SELECT toUInt64(number) FROM numbers(2);
SYSTEM FLUSH DISTRIBUTED dist_test_01040;
SELECT * FROM dist_test_01040 ORDER BY key;
TRUNCATE TABLE test_01040;

SELECT 'test_cluster_two_shards_internal_replication prefer_localhost_replica=1';
SET prefer_localhost_replica=1;
INSERT INTO dist_test_01040 SELECT toUInt64(number) FROM numbers(2);
SYSTEM FLUSH DISTRIBUTED dist_test_01040;
SELECT * FROM dist_test_01040 ORDER BY key;
TRUNCATE TABLE test_01040;


DROP TABLE dist_test_01040;
DROP TABLE test_01040;
