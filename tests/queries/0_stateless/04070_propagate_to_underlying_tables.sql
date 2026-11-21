DROP TABLE IF EXISTS local_04070 ON CLUSTER 'test_shard_localhost' SYNC;
DROP TABLE IF EXISTS dist_04070;
DROP TABLE IF EXISTS non_dist_04070;

CREATE TABLE local_04070 ON CLUSTER 'test_shard_localhost'
(
    id UInt64
)
ENGINE = MergeTree
ORDER BY id;

CREATE TABLE dist_04070
(
    id UInt64
)
ENGINE = Distributed('test_shard_localhost', currentDatabase(), 'local_04070', rand());

CREATE TABLE non_dist_04070
(
    id UInt64
)
ENGINE = Memory;

ALTER TABLE non_dist_04070 ADD COLUMN s String SETTINGS alter_distributed_propagate_to_remote = 1; -- { serverError BAD_ARGUMENTS }
ALTER TABLE dist_04070 ADD COLUMN no_cluster String SETTINGS alter_distributed_propagate_to_remote = 1; -- { serverError BAD_ARGUMENTS }
ALTER TABLE dist_04070 DROP COLUMN id SETTINGS alter_distributed_propagate_to_remote = 1; -- { serverError NOT_IMPLEMENTED }

ALTER TABLE dist_04070 ON CLUSTER 'test_shard_localhost' ADD COLUMN s String DEFAULT toString(id) SETTINGS alter_distributed_propagate_to_remote = 1, distributed_ddl_task_timeout = 30;
ALTER TABLE dist_04070 ON CLUSTER 'test_shard_localhost' COMMENT COLUMN s 'new comment' SETTINGS alter_distributed_propagate_to_remote = 1, distributed_ddl_task_timeout = 30;

DESCRIBE TABLE dist_04070;

SELECT comment
FROM system.columns
WHERE database = currentDatabase()
    AND table = 'dist_04070'
    AND name = 's';

SELECT count()
FROM clusterAllReplicas('test_shard_localhost', system.columns)
WHERE database = currentDatabase()
    AND table = 'local_04070'
    AND name = 's';

SELECT distinct comment
FROM clusterAllReplicas('test_shard_localhost', system.columns)
WHERE database = currentDatabase()
    AND table = 'local_04070'
    AND name = 's';

DROP TABLE dist_04070;
DROP TABLE non_dist_04070;
DROP TABLE local_04070 ON CLUSTER 'test_shard_localhost' SYNC;
