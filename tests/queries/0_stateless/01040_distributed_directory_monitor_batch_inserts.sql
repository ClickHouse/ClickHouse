SET distributed_directory_monitor_batch_inserts=1;
SET distributed_directory_monitor_sleep_time_ms=10;
SET distributed_directory_monitor_max_sleep_time_ms=100;

DROP TABLE IF EXISTS test_01040;
DROP TABLE IF EXISTS dist_test_01040;

CREATE TABLE test_01040 (key UInt64) ENGINE=TinyLog();
CREATE TABLE dist_test_01040 AS test_01040 Engine=Distributed(test_cluster_two_shards, currentDatabase(), test_01040, key);
INSERT INTO dist_test_01040 SELECT toUInt64(number) FROM numbers(2);
SYSTEM FLUSH DISTRIBUTED dist_test_01040;
SELECT * FROM dist_test_01040;
