-- Tags: distributed

SET send_logs_level = 'fatal';
SET prefer_localhost_replica = 1;
SET parallel_distributed_insert_select = 2;

DROP TABLE IF EXISTS local_03990_src;
DROP TABLE IF EXISTS local_03990_dst;
DROP TABLE IF EXISTS distributed_03990_src;
DROP TABLE IF EXISTS distributed_03990_dst;

CREATE TABLE local_03990_src (col String) ENGINE = MergeTree ORDER BY col;
CREATE TABLE local_03990_dst (col String) ENGINE = MergeTree ORDER BY col;
CREATE TABLE distributed_03990_src AS local_03990_src ENGINE = Distributed('test_shard_localhost', currentDatabase(), local_03990_src);
CREATE TABLE distributed_03990_dst AS local_03990_dst ENGINE = Distributed('test_shard_localhost', currentDatabase(), local_03990_dst);

INSERT INTO local_03990_src VALUES ('value1'), ('value2');

INSERT INTO distributed_03990_dst
WITH cte AS (SELECT * FROM distributed_03990_src) SELECT cte.col FROM cte AS c;

SELECT * FROM local_03990_dst ORDER BY col;
TRUNCATE TABLE local_03990_dst;

INSERT INTO distributed_03990_dst
WITH cte AS (SELECT * FROM distributed_03990_src) SELECT c.col FROM cte AS c;

SELECT * FROM local_03990_dst ORDER BY col;
TRUNCATE TABLE local_03990_dst;

INSERT INTO distributed_03990_dst
WITH cte AS (SELECT * FROM distributed_03990_src) SELECT cte.col FROM cte;

SELECT * FROM local_03990_dst ORDER BY col;

DROP TABLE local_03990_src;
DROP TABLE local_03990_dst;
DROP TABLE distributed_03990_src;
DROP TABLE distributed_03990_dst;
