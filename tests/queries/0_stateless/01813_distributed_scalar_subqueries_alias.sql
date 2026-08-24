-- Tags: distributed

DROP TABLE IF EXISTS data;
CREATE TABLE data (a Int64, b Int64) ENGINE = TinyLog();

DROP TABLE IF EXISTS data_distributed;
CREATE TABLE data_distributed (a Int64, b Int64) ENGINE = Distributed(test_shard_localhost, currentDatabase(), 'data');

INSERT INTO data VALUES (0, 0);

SET prefer_localhost_replica = 1;
SELECT a / (SELECT sum(number) FROM numbers(10)) FROM data_distributed;
SELECT a < (SELECT 1) FROM data_distributed;

SET prefer_localhost_replica = 0;
SELECT a / (SELECT sum(number) FROM numbers(10)) FROM data_distributed;
SELECT a < (SELECT 1) FROM data_distributed;

DROP TABLE data_distributed;
DROP TABLE data;
