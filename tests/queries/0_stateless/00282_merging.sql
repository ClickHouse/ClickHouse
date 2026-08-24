DROP TABLE IF EXISTS merge;

set allow_deprecated_syntax_for_merge_tree=1;
CREATE TABLE IF NOT EXISTS merge (d Date DEFAULT '2000-01-01', x UInt64) ENGINE = MergeTree(d, x, 5);

INSERT INTO merge (x) VALUES (1), (2), (3);
INSERT INTO merge (x) VALUES (4), (5), (6);

SELECT * FROM merge ORDER BY _part_index, x;
OPTIMIZE TABLE merge;
SELECT * FROM merge ORDER BY _part_index, x;

DROP TABLE merge;


CREATE TABLE IF NOT EXISTS merge (d Date DEFAULT '2000-01-01', x UInt64) ENGINE = MergeTree(d, x, 5);

INSERT INTO merge (x) SELECT number      AS x FROM system.numbers LIMIT 10;
INSERT INTO merge (x) SELECT number + 10 AS x FROM system.numbers LIMIT 10;

SELECT * FROM merge ORDER BY _part_index, x;
OPTIMIZE TABLE merge;
SELECT * FROM merge ORDER BY _part_index, x;

DROP TABLE merge;


CREATE TABLE IF NOT EXISTS merge (d Date DEFAULT '2000-01-01', x UInt64) ENGINE = MergeTree(d, x, 5);

INSERT INTO merge (x) SELECT number + 5 AS x FROM system.numbers LIMIT 10;
INSERT INTO merge (x) SELECT number     AS x FROM system.numbers LIMIT 10;

SELECT * FROM merge ORDER BY _part_index, x;
OPTIMIZE TABLE merge;
SELECT * FROM merge ORDER BY _part_index, x;

DROP TABLE merge;


CREATE TABLE IF NOT EXISTS merge (d Date DEFAULT '2000-01-01', x UInt64) ENGINE = MergeTree(d, x, 5);

INSERT INTO merge (x) SELECT number + 5 AS x FROM system.numbers LIMIT 10;
INSERT INTO merge (x) SELECT number     AS x FROM system.numbers LIMIT 10;
INSERT INTO merge (x) SELECT number + 9 AS x FROM system.numbers LIMIT 10;

SELECT * FROM merge ORDER BY _part_index, x;
OPTIMIZE TABLE merge;
SELECT * FROM merge ORDER BY _part_index, x;

DROP TABLE merge;


CREATE TABLE IF NOT EXISTS merge (d Date DEFAULT '2000-01-01', x UInt64) ENGINE = MergeTree(d, x, 5);

INSERT INTO merge (x) SELECT number      AS x FROM system.numbers LIMIT 10;
INSERT INTO merge (x) SELECT number + 5  AS x FROM system.numbers LIMIT 10;
INSERT INTO merge (x) SELECT number + 10 AS x FROM system.numbers LIMIT 10;

SELECT * FROM merge ORDER BY _part_index, x;
OPTIMIZE TABLE merge;
SELECT * FROM merge ORDER BY _part_index, x;

INSERT INTO merge (x) SELECT number + 5  AS x FROM system.numbers LIMIT 10;

SELECT * FROM merge ORDER BY _part_index, x;
OPTIMIZE TABLE merge;
SELECT * FROM merge ORDER BY _part_index, x;

INSERT INTO merge (x) SELECT number + 100  AS x FROM system.numbers LIMIT 10;

SELECT * FROM merge ORDER BY _part_index, x;
OPTIMIZE TABLE merge;
SELECT * FROM merge ORDER BY _part_index, x;

DROP TABLE merge;


set allow_deprecated_syntax_for_merge_tree=1;
CREATE TABLE IF NOT EXISTS merge (d Date DEFAULT '2000-01-01', x UInt64) ENGINE = MergeTree(d, x, 8192);

SET min_insert_block_size_rows = 0, min_insert_block_size_bytes = 0;
SET max_block_size = 8200;
INSERT INTO merge (x) SELECT number AS x FROM (SELECT * FROM system.numbers LIMIT 8200) ORDER BY rand();
INSERT INTO merge (x) SELECT number AS x FROM (SELECT * FROM system.numbers LIMIT 8200) ORDER BY rand();

OPTIMIZE TABLE merge;

SELECT count(), uniqExact(x), min(x), max(x), sum(x), sum(cityHash64(x)) FROM merge;

DROP TABLE merge;


CREATE TABLE IF NOT EXISTS merge (d Date DEFAULT '2000-01-01', x UInt64) ENGINE = MergeTree(d, x, 8192);

SET max_block_size = 10000;
INSERT INTO merge (x) SELECT number AS x FROM (SELECT number FROM system.numbers LIMIT 10000);
INSERT INTO merge (x) SELECT number AS x FROM (SELECT number + 5000 AS number FROM system.numbers LIMIT 10000);

OPTIMIZE TABLE merge;

SELECT count(), uniqExact(x), min(x), max(x), sum(x), sum(cityHash64(x)) FROM merge;

DROP TABLE merge;
