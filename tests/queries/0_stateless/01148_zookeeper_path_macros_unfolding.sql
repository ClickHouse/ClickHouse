-- Tags: zookeeper, no-replicated-database, no-parallel

DROP TABLE IF EXISTS rmt;

CREATE TABLE rmt (n UInt64, s String) ENGINE = ReplicatedMergeTree('/clickhouse/test_01148/{shard}/{database}/{table}', '{replica}') ORDER BY n;
SHOW CREATE TABLE rmt;
RENAME TABLE rmt TO rmt1;
DETACH TABLE rmt1;
ATTACH TABLE rmt1;
SHOW CREATE TABLE rmt1;

CREATE TABLE rmt (n UInt64, s String) ENGINE = ReplicatedMergeTree('{default_path_test}{uuid}', '{default_name_test}') ORDER BY n;    -- { serverError 62 }
CREATE TABLE rmt (n UInt64, s String) ENGINE = ReplicatedMergeTree('{default_path_test}test_01148', '{default_name_test}') ORDER BY n;
SHOW CREATE TABLE rmt;
RENAME TABLE rmt TO rmt2;   -- { serverError 48 }
DETACH TABLE rmt;
ATTACH TABLE rmt;
SHOW CREATE TABLE rmt;

DROP TABLE rmt;
DROP TABLE rmt1;
