-- Tags: no-parallel

CREATE DATABASE rdb1 ENGINE = Replicated('/test/test_replication_lag_metric', 'shard1', 'replica1');
CREATE DATABASE rdb2 ENGINE = Replicated('/test/test_replication_lag_metric', 'shard1', 'replica2');

SET distributed_ddl_task_timeout = 0;
CREATE TABLE rdb1.t (id UInt32) ENGINE = ReplicatedMergeTree ORDER BY id;
SELECT replication_lag FROM system.clusters WHERE cluster IN ('rdb1', 'rdb2') ORDER BY cluster ASC, replica_num ASC;

DROP DATABASE rdb1;
DROP DATABASE rdb2;
