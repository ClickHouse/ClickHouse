SET enable_analyzer = 1;

DROP TABLE IF EXISTS source_table1;
DROP TABLE IF EXISTS source_table2;
DROP TABLE IF EXISTS distributed_table1;
DROP TABLE IF EXISTS distributed_table2;

CREATE TABLE source_table1 (a Int64, b String) ENGINE = MergeTree ORDER BY a;
CREATE TABLE source_table2 (c Int64, d String) ENGINE = MergeTree ORDER BY c;

INSERT INTO source_table1 VALUES (42, 'qwe');
INSERT INTO source_table2 VALUES (42, 'asd');

CREATE TABLE distributed_table1 AS source_table1
ENGINE = Distributed('test_shard_localhost', currentDatabase(), source_table1);

CREATE TABLE distributed_table2 AS source_table2
ENGINE = Distributed('test_shard_localhost', currentDatabase(), source_table2);

SELECT 1 FROM distributed_table1 AS t1 GLOBAL JOIN distributed_table2 AS t2 ON materialize(42) = t1.a;
SELECT count() FROM distributed_table1 AS t1 GLOBAL JOIN distributed_table2 AS t2 ON materialize(42) = t1.a;
SELECT t1.* FROM distributed_table1 AS t1 GLOBAL JOIN distributed_table2 AS t2 ON materialize(42) = t1.a;
SELECT t2.* FROM distributed_table1 AS t1 GLOBAL JOIN distributed_table2 AS t2 ON materialize(42) = t1.a;
