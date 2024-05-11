SET allow_experimental_analyzer = 1;

DROP TABLE IF EXISTS m;
DROP TABLE IF EXISTS d1;
DROP TABLE IF EXISTS d2;
DROP TABLE IF EXISTS d3;

CREATE TABLE d1 (key Int, value Int) ENGINE=Memory();
CREATE TABLE d2 (key Int, value Int) ENGINE=MergeTree() ORDER BY key;
CREATE TABLE d3 (_table Int, value Int) ENGINE=Memory();

INSERT INTO d1 VALUES (1, 10);
INSERT INTO d1 VALUES (2, 20);

INSERT INTO d2 VALUES (3, 30);
INSERT INTO d2 VALUES (4, 40);

INSERT INTO d3 VALUES (5, 50);
INSERT INTO d3 VALUES (6, 60);

CREATE TABLE m AS v1 ENGINE=Merge(currentDatabase(), '^(d1|d2)$');

-- { echoOn }
SELECT _table FROM d1;
SELECT count(_table) FROM d1 WHERE _table = 'd1' GROUP BY _table;
SELECT _table, key, value FROM d1 WHERE value = 10;

SELECT _table FROM d2;
SELECT count(_table) FROM d2 WHERE _table = 'd2' GROUP BY _table;
SELECT _table, key, value FROM d2 WHERE value = 40;

SELECT _table, value FROM d3 WHERE _table = 6;

SELECT _table FROM m ORDER BY _table ASC;
SELECT count(_table) FROM m WHERE _table = 'd1' GROUP BY _table;
SELECT _table, key, value FROM m WHERE _table = 'd2' and value <= 30;

SELECT _table, key, value FROM (SELECT _table, key, value FROM d1 UNION ALL SELECT _table, key, value FROM d2) ORDER BY key ASC;
