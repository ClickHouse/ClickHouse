DROP TABLE IF EXISTS test.merge;
CREATE TABLE IF NOT EXISTS test.merge (d Date DEFAULT '2000-01-01', x UInt64) ENGINE = MergeTree(d, x, 5);

INSERT INTO test.merge (x) VALUES (1), (2), (3);
INSERT INTO test.merge (x) VALUES (4), (5), (6);

SELECT * FROM test.merge ORDER BY _part_index, x;
OPTIMIZE TABLE test.merge;
SELECT * FROM test.merge ORDER BY _part_index, x;

DROP TABLE test.merge;


CREATE TABLE IF NOT EXISTS test.merge (d Date DEFAULT '2000-01-01', x UInt64) ENGINE = MergeTree(d, x, 5);

INSERT INTO test.merge (x) SELECT number      AS x FROM system.numbers LIMIT 10;
INSERT INTO test.merge (x) SELECT number + 10 AS x FROM system.numbers LIMIT 10;

SELECT * FROM test.merge ORDER BY _part_index, x;
OPTIMIZE TABLE test.merge;
SELECT * FROM test.merge ORDER BY _part_index, x;

DROP TABLE test.merge;


CREATE TABLE IF NOT EXISTS test.merge (d Date DEFAULT '2000-01-01', x UInt64) ENGINE = MergeTree(d, x, 5);

INSERT INTO test.merge (x) SELECT number + 5 AS x FROM system.numbers LIMIT 10;
INSERT INTO test.merge (x) SELECT number     AS x FROM system.numbers LIMIT 10;

SELECT * FROM test.merge ORDER BY _part_index, x;
OPTIMIZE TABLE test.merge;
SELECT * FROM test.merge ORDER BY _part_index, x;

DROP TABLE test.merge;


CREATE TABLE IF NOT EXISTS test.merge (d Date DEFAULT '2000-01-01', x UInt64) ENGINE = MergeTree(d, x, 5);

INSERT INTO test.merge (x) SELECT number + 5 AS x FROM system.numbers LIMIT 10;
INSERT INTO test.merge (x) SELECT number     AS x FROM system.numbers LIMIT 10;
INSERT INTO test.merge (x) SELECT number + 9 AS x FROM system.numbers LIMIT 10;

SELECT * FROM test.merge ORDER BY _part_index, x;
OPTIMIZE TABLE test.merge;
SELECT * FROM test.merge ORDER BY _part_index, x;

DROP TABLE test.merge;


CREATE TABLE IF NOT EXISTS test.merge (d Date DEFAULT '2000-01-01', x UInt64) ENGINE = MergeTree(d, x, 5);

INSERT INTO test.merge (x) SELECT number      AS x FROM system.numbers LIMIT 10;
INSERT INTO test.merge (x) SELECT number + 5  AS x FROM system.numbers LIMIT 10;
INSERT INTO test.merge (x) SELECT number + 10 AS x FROM system.numbers LIMIT 10;

SELECT * FROM test.merge ORDER BY _part_index, x;
OPTIMIZE TABLE test.merge;
SELECT * FROM test.merge ORDER BY _part_index, x;

INSERT INTO test.merge (x) SELECT number + 5  AS x FROM system.numbers LIMIT 10;

SELECT * FROM test.merge ORDER BY _part_index, x;
OPTIMIZE TABLE test.merge;
SELECT * FROM test.merge ORDER BY _part_index, x;

INSERT INTO test.merge (x) SELECT number + 100  AS x FROM system.numbers LIMIT 10;

SELECT * FROM test.merge ORDER BY _part_index, x;
OPTIMIZE TABLE test.merge;
SELECT * FROM test.merge ORDER BY _part_index, x;

DROP TABLE test.merge;


CREATE TABLE IF NOT EXISTS test.merge (d Date DEFAULT '2000-01-01', x UInt64) ENGINE = MergeTree(d, x, 8192);

SET min_insert_block_size_rows = 0, min_insert_block_size_bytes = 0;
SET max_block_size = 8200;
INSERT INTO test.merge (x) SELECT number AS x FROM (SELECT * FROM system.numbers LIMIT 8200) ORDER BY rand();
INSERT INTO test.merge (x) SELECT number AS x FROM (SELECT * FROM system.numbers LIMIT 8200) ORDER BY rand();

OPTIMIZE TABLE test.merge;

SELECT count(), uniqExact(x), min(x), max(x), sum(x), sum(cityHash64(x)) FROM test.merge;

DROP TABLE test.merge;


CREATE TABLE IF NOT EXISTS test.merge (d Date DEFAULT '2000-01-01', x UInt64) ENGINE = MergeTree(d, x, 8192);

SET max_block_size = 10000;
INSERT INTO test.merge (x) SELECT number AS x FROM (SELECT number FROM system.numbers LIMIT 10000);
INSERT INTO test.merge (x) SELECT number AS x FROM (SELECT number + 5000 AS number FROM system.numbers LIMIT 10000);

OPTIMIZE TABLE test.merge;

SELECT count(), uniqExact(x), min(x), max(x), sum(x), sum(cityHash64(x)) FROM test.merge;

DROP TABLE test.merge;
