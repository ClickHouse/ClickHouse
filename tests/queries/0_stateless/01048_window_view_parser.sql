-- Tags: no-parallel

SET send_logs_level = 'fatal';
SET enable_analyzer = 0;
SET allow_experimental_window_view = 1;
DROP DATABASE IF EXISTS test_01048;
set allow_deprecated_database_ordinary=1;
-- Creation of a database with Ordinary engine emits a warning.
CREATE DATABASE test_01048 ENGINE=Ordinary;

DROP TABLE IF EXISTS test_01048.mt;
DROP TABLE IF EXISTS test_01048.mt_2;

CREATE TABLE test_01048.mt(a Int32, b Int32, timestamp DateTime) ENGINE=MergeTree ORDER BY tuple();
CREATE TABLE test_01048.mt_2(a Int32, b Int32, timestamp DateTime) ENGINE=MergeTree ORDER BY tuple();

SELECT '---TUMBLE---';
SELECT '||---WINDOW COLUMN NAME---';
DROP TABLE IF EXISTS test_01048.wv;
CREATE WINDOW VIEW test_01048.wv ENGINE Memory AS SELECT count(a) AS count, tumbleEnd(wid) as wend FROM test_01048.mt GROUP BY tumble(timestamp, INTERVAL 1 SECOND) as wid;
SHOW CREATE TABLE test_01048.`.inner.wv`;

SELECT '||---WINDOW COLUMN ALIAS---';
DROP TABLE IF EXISTS test_01048.wv;
CREATE WINDOW VIEW test_01048.wv ENGINE Memory AS SELECT count(a) AS count, tumble(timestamp, INTERVAL '1' SECOND) AS wid FROM test_01048.mt GROUP BY wid;
SHOW CREATE TABLE test_01048.`.inner.wv`;

SELECT '||---IDENTIFIER---';
DROP TABLE IF EXISTS test_01048.wv;
CREATE WINDOW VIEW test_01048.wv ENGINE Memory AS SELECT count(a) AS count FROM test_01048.mt GROUP BY b, tumble(timestamp, INTERVAL '1' SECOND) AS wid;
SHOW CREATE TABLE test_01048.`.inner.wv`;

DROP TABLE IF EXISTS test_01048.wv;
CREATE WINDOW VIEW test_01048.wv ENGINE Memory AS SELECT count(a) AS count FROM test_01048.mt GROUP BY tumble(timestamp, INTERVAL '1' SECOND) AS wid, b;
SHOW CREATE TABLE test_01048.`.inner.wv`;

SELECT '||---FUNCTION---';
DROP TABLE IF EXISTS test_01048.wv;
CREATE WINDOW VIEW test_01048.wv ENGINE Memory AS SELECT count(a) AS count FROM test_01048.mt GROUP BY plus(a, b) as _type, tumble(timestamp, INTERVAL '1' SECOND) AS wid;
SHOW CREATE TABLE test_01048.`.inner.wv`;

DROP TABLE IF EXISTS test_01048.wv;
CREATE WINDOW VIEW test_01048.wv ENGINE Memory AS SELECT count(a) AS count FROM test_01048.mt GROUP BY tumble(timestamp, INTERVAL '1' SECOND) AS wid, plus(a, b);
SHOW CREATE TABLE test_01048.`.inner.wv`;

SELECT '||---TimeZone---';
DROP TABLE IF EXISTS test_01048.wv;
CREATE WINDOW VIEW test_01048.wv ENGINE Memory AS SELECT count(a) AS count, tumble(timestamp, INTERVAL '1' SECOND, 'Asia/Shanghai') AS wid FROM test_01048.mt GROUP BY wid;
SHOW CREATE TABLE test_01048.`.inner.wv`;

SELECT '||---DATA COLUMN ALIAS---';
DROP TABLE IF EXISTS test_01048.wv;
CREATE WINDOW VIEW test_01048.wv ENGINE Memory AS SELECT count(a) AS count, b as id FROM test_01048.mt GROUP BY id, tumble(timestamp, INTERVAL '1' SECOND);
SHOW CREATE TABLE test_01048.`.inner.wv`;

SELECT '||---JOIN---';
DROP TABLE IF EXISTS test_01048.wv;
CREATE WINDOW VIEW test_01048.wv ENGINE Memory AS SELECT count(test_01048.mt.a), count(test_01048.mt_2.b), wid FROM test_01048.mt JOIN test_01048.mt_2 ON test_01048.mt.timestamp = test_01048.mt_2.timestamp GROUP BY tumble(test_01048.mt.timestamp, INTERVAL '1' SECOND) AS wid;
SHOW CREATE TABLE test_01048.`.inner.wv`;

SELECT '||---POPULATE JOIN---';
DROP TABLE IF EXISTS test_01048.wv;
CREATE WINDOW VIEW test_01048.wv ENGINE Memory POPULATE AS SELECT count(test_01048.mt.a), count(test_01048.mt_2.b), wid FROM test_01048.mt JOIN test_01048.mt_2 ON test_01048.mt.timestamp = test_01048.mt_2.timestamp GROUP BY tumble(test_01048.mt.timestamp, INTERVAL '1' SECOND) AS wid;
SHOW CREATE TABLE test_01048.`.inner.wv`;


SELECT '---HOP---';
SELECT '||---WINDOW COLUMN NAME---';
DROP TABLE IF EXISTS test_01048.wv;
CREATE WINDOW VIEW test_01048.wv ENGINE Memory AS SELECT count(a) AS count, hopEnd(wid) as wend FROM test_01048.mt GROUP BY hop(timestamp, INTERVAL 1 SECOND, INTERVAL 3 SECOND) as wid;
SHOW CREATE TABLE test_01048.`.inner.wv`;

SELECT '||---WINDOW COLUMN ALIAS---';
DROP TABLE IF EXISTS test_01048.wv;
CREATE WINDOW VIEW test_01048.wv ENGINE Memory AS SELECT count(a) AS count, hop(timestamp, INTERVAL '1' SECOND, INTERVAL '3' SECOND) AS wid FROM test_01048.mt GROUP BY wid;
SHOW CREATE TABLE test_01048.`.inner.wv`;

SELECT '||---IDENTIFIER---';
DROP TABLE IF EXISTS test_01048.wv;
CREATE WINDOW VIEW test_01048.wv ENGINE Memory AS SELECT count(a) AS count FROM test_01048.mt GROUP BY b, hop(timestamp, INTERVAL '1' SECOND, INTERVAL '3' SECOND) AS wid;
SHOW CREATE TABLE test_01048.`.inner.wv`;

DROP TABLE IF EXISTS test_01048.wv;
CREATE WINDOW VIEW test_01048.wv ENGINE Memory AS SELECT count(a) AS count FROM test_01048.mt GROUP BY hop(timestamp, INTERVAL '1' SECOND, INTERVAL '3' SECOND) AS wid, b;
SHOW CREATE TABLE test_01048.`.inner.wv`;

SELECT '||---FUNCTION---';
DROP TABLE IF EXISTS test_01048.wv;
CREATE WINDOW VIEW test_01048.wv ENGINE Memory AS SELECT count(a) AS count FROM test_01048.mt GROUP BY plus(a, b) as _type, hop(timestamp, INTERVAL '1' SECOND, INTERVAL '3' SECOND) AS wid;
SHOW CREATE TABLE test_01048.`.inner.wv`;

DROP TABLE IF EXISTS test_01048.wv;
CREATE WINDOW VIEW test_01048.wv ENGINE Memory AS SELECT count(a) AS count FROM test_01048.mt GROUP BY hop(timestamp, INTERVAL '1' SECOND, INTERVAL '3' SECOND) AS wid, plus(a, b);
SHOW CREATE TABLE test_01048.`.inner.wv`;

SELECT '||---TimeZone---';
DROP TABLE IF EXISTS test_01048.wv;
CREATE WINDOW VIEW test_01048.wv ENGINE Memory AS SELECT count(a) AS count, hopEnd(wid) as wend FROM test_01048.mt GROUP BY hop(timestamp, INTERVAL 1 SECOND, INTERVAL 3 SECOND, 'Asia/Shanghai') as wid;
SHOW CREATE TABLE test_01048.`.inner.wv`;

SELECT '||---DATA COLUMN ALIAS---';
DROP TABLE IF EXISTS test_01048.wv;
CREATE WINDOW VIEW test_01048.wv ENGINE Memory AS SELECT count(a) AS count, b as id FROM test_01048.mt GROUP BY id, hop(timestamp, INTERVAL '1' SECOND, INTERVAL '3' SECOND);
SHOW CREATE TABLE test_01048.`.inner.wv`;

SELECT '||---JOIN---';
DROP TABLE IF EXISTS test_01048.wv;
CREATE WINDOW VIEW test_01048.wv ENGINE Memory AS SELECT count(test_01048.mt.a), count(test_01048.mt_2.b), wid FROM test_01048.mt JOIN test_01048.mt_2 ON test_01048.mt.timestamp = test_01048.mt_2.timestamp GROUP BY hop(test_01048.mt.timestamp, INTERVAL '1' SECOND, INTERVAL '3' SECOND) AS wid;
SHOW CREATE TABLE test_01048.`.inner.wv`;

SELECT '||---POPULATE JOIN---';
DROP TABLE IF EXISTS test_01048.wv;
CREATE WINDOW VIEW test_01048.wv ENGINE Memory POPULATE AS SELECT count(test_01048.mt.a), count(test_01048.mt_2.b), wid FROM test_01048.mt JOIN test_01048.mt_2 ON test_01048.mt.timestamp = test_01048.mt_2.timestamp GROUP BY hop(test_01048.mt.timestamp, INTERVAL '1' SECOND, INTERVAL '3' SECOND) AS wid;
SHOW CREATE TABLE test_01048.`.inner.wv`;

DROP TABLE test_01048.wv;
DROP TABLE test_01048.mt;
DROP TABLE test_01048.mt_2;
