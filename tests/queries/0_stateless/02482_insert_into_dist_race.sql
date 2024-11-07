DROP TABLE IF EXISTS tmp_02482;
DROP TABLE IF EXISTS dist_02482;

-- This test produces warning
SET send_logs_level = 'error';
SET prefer_localhost_replica=0;

CREATE TABLE tmp_02482 (i UInt64, n LowCardinality(String)) ENGINE = Memory;
CREATE TABLE dist_02482(i UInt64, n LowCardinality(Nullable(String))) ENGINE = Distributed(test_cluster_two_shards, currentDatabase(), tmp_02482, i);

SET distributed_foreground_insert=1;

INSERT INTO dist_02482 VALUES (1, '1'), (2, '2');
INSERT INTO dist_02482 SELECT number, number FROM numbers(1000);

SET distributed_foreground_insert=0;

SYSTEM STOP DISTRIBUTED SENDS dist_02482;

INSERT INTO dist_02482 VALUES (1, '1'),(2, '2');
INSERT INTO dist_02482 SELECT number, number FROM numbers(1000);

SYSTEM FLUSH DISTRIBUTED dist_02482;

DROP TABLE tmp_02482;
DROP TABLE dist_02482;
