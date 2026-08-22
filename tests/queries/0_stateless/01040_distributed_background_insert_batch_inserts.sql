-- Tags: distributed

DROP TABLE IF EXISTS test_01040;
DROP TABLE IF EXISTS dist_test_01040;

CREATE TABLE test_01040 (key UInt64) ENGINE=TinyLog();
CREATE TABLE dist_test_01040 AS test_01040 Engine=Distributed(test_cluster_two_shards, currentDatabase(), test_01040, key) SETTINGS
    background_insert_batch=1,
    background_insert_sleep_time_ms=10,
    background_insert_max_sleep_time_ms=100;

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
CREATE TABLE dist_test_01040 AS test_01040 Engine=Distributed(test_cluster_two_shards_internal_replication, currentDatabase(), test_01040, key) SETTINGS
    background_insert_batch=1,
    background_insert_sleep_time_ms=10,
    background_insert_max_sleep_time_ms=100;
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
