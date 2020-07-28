SET distributed_directory_monitor_batch_inserts=1;
SET distributed_directory_monitor_sleep_time_ms=10;
SET distributed_directory_monitor_max_sleep_time_ms=100;

CREATE TABLE test (key UInt64) ENGINE=TinyLog();
CREATE TABLE dist_test AS test Engine=Distributed(test_cluster_two_shards, currentDatabase(), test, key);
INSERT INTO dist_test SELECT toUInt64(number) FROM numbers(2);
SYSTEM FLUSH DISTRIBUTED dist_test;
SELECT * FROM dist_test;
