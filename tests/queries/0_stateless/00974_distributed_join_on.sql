-- Tags: distributed

DROP TABLE IF EXISTS source_table1;
DROP TABLE IF EXISTS source_table2;
DROP TABLE IF EXISTS distributed_table1;
DROP TABLE IF EXISTS distributed_table2;

CREATE TABLE source_table1 (a Int64, b String) ENGINE = Memory;
CREATE TABLE source_table2 (c Int64, d String) ENGINE = Memory;

INSERT INTO source_table1 VALUES (42, 'qwe');
INSERT INTO source_table2 VALUES (42, 'qwe');

CREATE TABLE distributed_table1 AS source_table1
ENGINE = Distributed('test_shard_localhost', currentDatabase(), source_table1);

CREATE TABLE distributed_table2 AS source_table2
ENGINE = Distributed('test_shard_localhost', currentDatabase(), source_table2);

SET prefer_localhost_replica = 1;
SELECT 1 FROM distributed_table1 AS t1 GLOBAL JOIN distributed_table2 AS t2 ON t1.a = t2.c LIMIT 1;
SELECT 1 FROM distributed_table1 AS t1 GLOBAL JOIN distributed_table2 AS t2 ON t2.c = t1.a LIMIT 1;
SELECT 1 FROM distributed_table1 AS t1 GLOBAL JOIN distributed_table1 AS t2 ON t1.a = t2.a LIMIT 1;

SET prefer_localhost_replica = 0;
SELECT 1 FROM distributed_table1 AS t1 GLOBAL JOIN distributed_table2 AS t2 ON t1.a = t2.c LIMIT 1;
SELECT 1 FROM distributed_table1 AS t1 GLOBAL JOIN distributed_table2 AS t2 ON t2.c = t1.a LIMIT 1;
SELECT 1 FROM distributed_table1 AS t1 GLOBAL JOIN distributed_table1 AS t2 ON t1.a = t2.a LIMIT 1;

SELECT t1.a as t1_a, t2.a as t2_a FROM source_table1 AS t1 JOIN source_table1 AS t2 ON t1_a = t2_a LIMIT 1;
SELECT t1.a as t1_a, t2.a as t2_a FROM distributed_table1 AS t1 GLOBAL JOIN distributed_table1 AS t2 ON t1_a = t2_a LIMIT 1;

DROP TABLE source_table1;
DROP TABLE source_table2;
DROP TABLE distributed_table1;
DROP TABLE distributed_table2;
