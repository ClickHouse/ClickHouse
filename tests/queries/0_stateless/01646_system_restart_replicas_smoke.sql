-- Tags: replica, no-tsan, no-parallel, no-stress
-- Tag no-tsan: RESTART REPLICAS can acquire too much locks, while only 64 is possible from one thread under TSan
-- Tag no-stress: RESTART REPLICAS can leave some tables,
--                that may pollute error log,
--                like in 01414_mutations_and_errors_zookeeper.
--                no-stress is like worked no-parallel for stress testing

DROP TABLE IF EXISTS data_01646;
CREATE TABLE data_01646 (x Date, s String) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/test_01646/data_01646', 'r') ORDER BY s PARTITION BY x;
SYSTEM RESTART REPLICAS;
DESCRIBE TABLE data_01646;
DROP TABLE data_01646;
