DROP TABLE IF EXISTS distributed_table_merged;
DROP TABLE IF EXISTS distributed_table_1;
DROP TABLE IF EXISTS distributed_table_2;
DROP TABLE IF EXISTS local_table_1;
DROP TABLE IF EXISTS local_table_2;
DROP TABLE IF EXISTS local_table_merged;

CREATE TABLE local_table_1 (id String) ENGINE = MergeTree ORDER BY (id);
CREATE TABLE local_table_2(id String) ENGINE = MergeTree ORDER BY (id);

CREATE TABLE local_table_merged (id String) ENGINE = Merge('default', 'local_table_1|local_table_2');

CREATE TABLE distributed_table_1 (id String) ENGINE = Distributed(test_shard_localhost, default, local_table_1);
CREATE TABLE distributed_table_2 (id String) ENGINE = Distributed(test_shard_localhost, default, local_table_2);

CREATE TABLE distributed_table_merged (id String) ENGINE = Merge('default', 'distributed_table_1|distributed_table_2');

SELECT 1 FROM distributed_table_merged;

DROP TABLE IF EXISTS distributed_table_merged;
DROP TABLE IF EXISTS distributed_table_1;
DROP TABLE IF EXISTS distributed_table_2;
DROP TABLE IF EXISTS local_table_1;
DROP TABLE IF EXISTS local_table_2;
