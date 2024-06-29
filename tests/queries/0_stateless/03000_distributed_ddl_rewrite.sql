DROP TABLE IF EXISTS local_table;
CREATE TABLE local_table (id Int64) ENGINE = MergeTree ORDER BY () PARTITION BY id % 5;

SELECT '----off----';

DROP TABLE IF EXISTS dist_table;
CREATE TABLE dist_table (id Int64) ENGINE = Distributed('test_shard_localhost', currentDatabase(), 'local_table');

OPTIMIZE TABLE dist_table; -- { serverError NOT_IMPLEMENTED }

ALTER TABLE dist_table DROP PARTITION 1; -- { serverError NOT_IMPLEMENTED }

ALTER TABLE dist_table ON CLUSTER test_shard_localhost MODIFY COLUMN id Int32;

SELECT '----on----';

DROP TABLE IF EXISTS dist_table;
CREATE TABLE dist_table (id Int64) ENGINE = Distributed('test_shard_localhost', currentDatabase(), 'local_table') SETTINGS enable_ddl_optimize_rewrite_to_oncluster = 1, enable_ddl_alter_rewrite_to_oncluster = 1;

OPTIMIZE TABLE dist_table;

ALTER TABLE dist_table DROP PARTITION 1;

ALTER TABLE dist_table ON CLUSTER test_shard_localhost DROP PARTITION 1; -- { serverError BAD_ARGUMENTS }

OPTIMIZE TABLE dist_table ON CLUSTER test_shard_localhost; -- { serverError BAD_ARGUMENTS }

ALTER TABLE dist_table ON CLUSTER test_shard_localhost MODIFY COLUMN id Int32;
