-- Tags: no-fasttest, no-azure-blob-storage, retry_ok

DROP TABLE IF EXISTS test_table_1;
CREATE TABLE test_table_1(number UInt64) ENGINE = MergeTree ORDER BY number;
SYSTEM STOP MERGES test_table_1;

DROP TABLE IF EXISTS dist_test_table_1;
CREATE TABLE dist_test_table_1(number UInt64) ENGINE = Distributed('test_cluster_five_shards_localhost', currentDatabase(), test_table_1, rand());
INSERT INTO dist_test_table_1 SELECT number from numbers_mt(10000) SETTINGS distributed_foreground_insert = 1;

DROP TABLE IF EXISTS test_table_2;
CREATE TABLE test_table_2(number UInt64) ENGINE = MergeTree ORDER BY number;
SYSTEM STOP MERGES test_table_2;

DROP TABLE IF EXISTS dist_test_table_2;
CREATE TABLE dist_test_table_2(number UInt64) ENGINE = Distributed('test_cluster_five_shards_localhost', currentDatabase(), test_table_2, rand());
INSERT INTO dist_test_table_2 SELECT number from numbers_mt(10000) SETTINGS distributed_foreground_insert = 1;

DROP TABLE IF EXISTS merge_test_table;
CREATE TABLE merge_test_table ENGINE = Merge(currentDatabase(), '^dist_test_table_(1|2)$');



DROP TABLE merge_test_table;
DROP TABLE dist_test_table_1;
DROP TABLE dist_test_table_2;
DROP TABLE test_table_1;
DROP TABLE test_table_2;
