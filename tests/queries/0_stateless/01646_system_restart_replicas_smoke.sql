-- Tags: replica, no-tsan, no-parallel
-- Tag no-tsan: RESTART REPLICAS can acquire too much locks, while only 64 is possible from one thread under TSan

DROP TABLE IF EXISTS data_01646;
CREATE TABLE data_01646 (x Date, s String) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/test_01646/data_01646', 'r') ORDER BY s PARTITION BY x;
SYSTEM RESTART REPLICAS;
DESCRIBE TABLE data_01646;
DROP TABLE data_01646;
