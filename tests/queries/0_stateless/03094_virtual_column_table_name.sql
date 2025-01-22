SET allow_experimental_analyzer = 1;

DROP TABLE IF EXISTS m1;
DROP TABLE IF EXISTS m2;
DROP TABLE IF EXISTS d1;
DROP TABLE IF EXISTS d2;
DROP TABLE IF EXISTS d3;
DROP TABLE IF EXISTS d4;
DROP TABLE IF EXISTS d5;
DROP TABLE IF EXISTS d6;
DROP TABLE IF EXISTS d7;
DROP TABLE IF EXISTS buffer1;
DROP VIEW IF EXISTS view1;
DROP VIEW IF EXISTS mv1;

CREATE TABLE d1 (key Int, value Int) ENGINE=Memory();
CREATE TABLE d2 (key Int, value Int) ENGINE=MergeTree() ORDER BY key;
CREATE TABLE d3 (_table Int, value Int) ENGINE=Memory();
CREATE TABLE d4 ENGINE=Distributed('test_shard_localhost', currentDatabase(), d2, rand());

INSERT INTO d1 VALUES (1, 10);
INSERT INTO d1 VALUES (2, 20);

INSERT INTO d2 VALUES (3, 30);
INSERT INTO d2 VALUES (4, 40);

INSERT INTO d3 VALUES (5, 50);
INSERT INTO d3 VALUES (6, 60);

CREATE TABLE m1 ENGINE=Merge(currentDatabase(), '^(d1|d2)$');
CREATE TABLE m2 ENGINE=Merge(currentDatabase(), '^(d1|d4)$');

CREATE VIEW view1 AS SELECT key, _table FROM d1;

CREATE TABLE d5 (key Int, value Int) ENGINE=MergeTree() ORDER BY key;
INSERT INTO d5 VALUES (7, 70);
INSERT INTO d5 VALUES (8, 80);
CREATE TABLE buffer1 AS d5 ENGINE = Buffer(currentDatabase(), d5, 1, 10000, 10000, 10000, 10000, 100000000, 100000000);
INSERT INTO buffer1 VALUES (9, 90);

CREATE TABLE d6 (key Int, value Int) ENGINE = MergeTree ORDER BY value;
CREATE TABLE d7 (key Int, value Int) ENGINE = SummingMergeTree ORDER BY key;
CREATE MATERIALIZED VIEW mv1 TO d7 AS SELECT key, count(value) AS value FROM d6 GROUP BY key;
INSERT INTO d6 VALUES (10, 100), (10, 110);

-- { echoOn }
SELECT _table FROM d1;
SELECT count(_table) FROM d1 WHERE _table = 'd1' GROUP BY _table;
SELECT _table, key, value FROM d1 WHERE value = 10;

SELECT _table FROM d2;
SELECT count(_table) FROM d2 WHERE _table = 'd2' GROUP BY _table;
SELECT _table, key, value FROM d2 WHERE value = 40;

SELECT _table, value FROM d3 WHERE _table = 6;

SELECT _table FROM d4;
SELECT count(_table) FROM d4 WHERE _table = 'd4' GROUP BY _table;
SELECT _table, key, value FROM d4 WHERE value = 40;

SELECT _table FROM m1 ORDER BY _table ASC;
SELECT count(_table) FROM m1 WHERE _table = 'd1' GROUP BY _table;
SELECT _table, key, value FROM m1 WHERE _table = 'd2' and value <= 30;

SELECT _table FROM m2 ORDER BY _table ASC;
SELECT count(_table) FROM m2 WHERE _table = 'd1' GROUP BY _table;
SELECT _table, key, value FROM m2 WHERE _table = 'd4' and value <= 30;

SELECT _table, key, value FROM (SELECT _table, key, value FROM d1 UNION ALL SELECT _table, key, value FROM d2) ORDER BY key ASC;

SELECT _table, key FROM view1;

SELECT _table, key, value FROM buffer1;

SELECT _table, key, value FROM mv1;
