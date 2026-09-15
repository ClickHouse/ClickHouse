-- Tags: replica, no-flaky-check

SET distributed_ddl_output_mode='none';
DROP TABLE IF EXISTS demo_loan_01568_dist;

CREATE DATABASE IF NOT EXISTS shard_0;
CREATE DATABASE IF NOT EXISTS shard_1;
DROP TABLE IF EXISTS shard_0.demo_loan_01568;
DROP TABLE IF EXISTS shard_1.demo_loan_01568;

CREATE TABLE demo_loan_01568 ON CLUSTER test_cluster_two_shards_different_databases ( `id` Int64 COMMENT 'id', `date_stat` Date COMMENT 'date of stat', `customer_no` String COMMENT 'customer no', `loan_principal` Float64 COMMENT 'loan principal' ) ENGINE=ReplacingMergeTree() ORDER BY id PARTITION BY toYYYYMM(date_stat); -- { serverError NOT_IMPLEMENTED }
SET distributed_ddl_entry_format_version = 2;
CREATE TABLE demo_loan_01568 ON CLUSTER test_cluster_two_shards_different_databases ( `id` Int64 COMMENT 'id', `date_stat` Date COMMENT 'date of stat', `customer_no` String COMMENT 'customer no', `loan_principal` Float64 COMMENT 'loan principal' ) ENGINE=ReplacingMergeTree() ORDER BY id PARTITION BY toYYYYMM(date_stat); -- { serverError INCONSISTENT_CLUSTER_DEFINITION }
SET distributed_ddl_output_mode='throw';
CREATE TABLE shard_0.demo_loan_01568 ON CLUSTER test_cluster_two_shards_different_databases ( `id` Int64 COMMENT 'id', `date_stat` Date COMMENT 'date of stat', `customer_no` String COMMENT 'customer no', `loan_principal` Float64 COMMENT 'loan principal' ) ENGINE=ReplacingMergeTree() ORDER BY id PARTITION BY toYYYYMM(date_stat);
CREATE TABLE shard_1.demo_loan_01568 ON CLUSTER test_cluster_two_shards_different_databases ( `id` Int64 COMMENT 'id', `date_stat` Date COMMENT 'date of stat', `customer_no` String COMMENT 'customer no', `loan_principal` Float64 COMMENT 'loan principal' ) ENGINE=ReplacingMergeTree() ORDER BY id PARTITION BY toYYYYMM(date_stat);
SET distributed_ddl_output_mode='none';

-- `shard_0`/`shard_1` are shared between tests, so the listing must be scoped to this test's table.
SHOW TABLES FROM shard_0 LIKE 'demo_loan_01568';
SHOW TABLES FROM shard_1 LIKE 'demo_loan_01568';
SHOW CREATE TABLE shard_0.demo_loan_01568;
SHOW CREATE TABLE shard_1.demo_loan_01568;

CREATE TABLE demo_loan_01568_dist AS shard_0.demo_loan_01568 ENGINE=Distributed('test_cluster_two_shards_different_databases', '', 'demo_loan_01568', id % 2);
INSERT INTO demo_loan_01568_dist VALUES (1, '2021-04-13', 'qwerty', 3.14159), (2, '2021-04-14', 'asdfgh', 2.71828);
SYSTEM FLUSH DISTRIBUTED demo_loan_01568_dist;
SELECT * FROM demo_loan_01568_dist ORDER BY id;

SELECT * FROM shard_0.demo_loan_01568;
SELECT * FROM shard_1.demo_loan_01568;

DROP TABLE demo_loan_01568_dist;
DROP TABLE shard_0.demo_loan_01568;
DROP TABLE shard_1.demo_loan_01568;
