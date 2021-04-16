DROP DATABASE IF EXISTS 01813_db;
CREATE DATABASE 01813_db;

DROP TABLE IF EXISTS 01813_db.data;
CREATE TABLE 01813_db.data (a Int64, b Int64) ENGINE = TinyLog();

DROP TABLE IF EXISTS 01813_db.data_distributed;
CREATE TABLE 01813_db.data_distributed (a Int64, b Int64) ENGINE = Distributed(test_shard_localhost, '01813_db', data);

INSERT INTO 01813_db.data VALUES (0, 0);

SET prefer_localhost_replica = 1;
SELECT a / (SELECT sum(number) FROM numbers(10)) FROM 01813_db.data_distributed;

SET prefer_localhost_replica = 0;
SELECT a / (SELECT sum(number) FROM numbers(10)) FROM 01813_db.data_distributed;

DROP TABLE 01813_db.data_distributed;
DROP TABLE 01813_db.data;
