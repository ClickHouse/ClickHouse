DROP TABLE IF EXISTS tmp_01683;
DROP TABLE IF EXISTS dist_01683;

SET prefer_localhost_replica=0;
-- To suppress "Structure does not match (remote: n Int8 Int8(size = 0), local: n UInt64 UInt64(size = 1)), implicit conversion will be done."
SET send_logs_level='error';

CREATE TABLE tmp_01683 (n Int8) ENGINE=Memory;
CREATE TABLE dist_01683 (n UInt64) Engine=Distributed(test_cluster_two_shards, currentDatabase(), tmp_01683, n);

SET insert_distributed_sync=1;
INSERT INTO dist_01683 VALUES (1),(2);

SET insert_distributed_sync=0;
INSERT INTO dist_01683 VALUES (1),(2);
SYSTEM FLUSH DISTRIBUTED dist_01683;

-- TODO: cover distributed_directory_monitor_batch_inserts=1

SELECT * FROM tmp_01683 ORDER BY n;

DROP TABLE tmp_01683;
DROP TABLE dist_01683;
