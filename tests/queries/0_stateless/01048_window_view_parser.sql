-- Tags: no-parallel

SET allow_experimental_window_view = 1;
DROP DATABASE IF EXISTS test_01048;
CREATE DATABASE test_01048 ENGINE=Ordinary;

DROP TABLE IF EXISTS test_01048.mt;

CREATE TABLE test_01048.mt(a Int32, b Int32, timestamp DateTime) ENGINE=MergeTree ORDER BY tuple();

SELECT '---TUMBLE---';
SELECT '||---WINDOW COLUMN NAME---';
DROP TABLE IF EXISTS test_01048.wv;
CREATE WINDOW VIEW test_01048.wv AS SELECT count(a) AS count, tumbleEnd(wid) as wend FROM test_01048.mt GROUP BY tumble(timestamp, INTERVAL 1 SECOND) as wid;
SHOW CREATE TABLE test_01048.`.inner.wv`;

SELECT '||---WINDOW COLUMN ALIAS---';
DROP TABLE IF EXISTS test_01048.wv;
CREATE WINDOW VIEW test_01048.wv AS SELECT count(a) AS count, tumble(timestamp, INTERVAL '1' SECOND) AS wid FROM test_01048.mt GROUP BY wid;
SHOW CREATE TABLE test_01048.`.inner.wv`;

SELECT '||---IDENTIFIER---';
DROP TABLE IF EXISTS test_01048.wv;
CREATE WINDOW VIEW test_01048.wv AS SELECT count(a) AS count FROM test_01048.mt GROUP BY b, tumble(timestamp, INTERVAL '1' SECOND) AS wid;
SHOW CREATE TABLE test_01048.`.inner.wv`;

DROP TABLE IF EXISTS test_01048.wv;
CREATE WINDOW VIEW test_01048.wv AS SELECT count(a) AS count FROM test_01048.mt GROUP BY tumble(timestamp, INTERVAL '1' SECOND) AS wid, b;
SHOW CREATE TABLE test_01048.`.inner.wv`;

SELECT '||---FUNCTION---';
DROP TABLE IF EXISTS test_01048.wv;
CREATE WINDOW VIEW test_01048.wv AS SELECT count(a) AS count FROM test_01048.mt GROUP BY plus(a, b) as _type, tumble(timestamp, INTERVAL '1' SECOND) AS wid;
SHOW CREATE TABLE test_01048.`.inner.wv`;

DROP TABLE IF EXISTS test_01048.wv;
CREATE WINDOW VIEW test_01048.wv AS SELECT count(a) AS count FROM test_01048.mt GROUP BY tumble(timestamp, INTERVAL '1' SECOND) AS wid, plus(a, b);
SHOW CREATE TABLE test_01048.`.inner.wv`;

SELECT '||---TimeZone---';
DROP TABLE IF EXISTS test_01048.wv;
CREATE WINDOW VIEW test_01048.wv AS SELECT count(a) AS count, tumble(timestamp, INTERVAL '1' SECOND, 'Asia/Shanghai') AS wid FROM test_01048.mt GROUP BY wid;
SHOW CREATE TABLE test_01048.`.inner.wv`;


SELECT '---HOP---';
SELECT '||---WINDOW COLUMN NAME---';
DROP TABLE IF EXISTS test_01048.wv;
CREATE WINDOW VIEW test_01048.wv AS SELECT count(a) AS count, hopEnd(wid) as wend FROM test_01048.mt GROUP BY hop(timestamp, INTERVAL 1 SECOND, INTERVAL 3 SECOND) as wid;
SHOW CREATE TABLE test_01048.`.inner.wv`;

SELECT '||---WINDOW COLUMN ALIAS---';
DROP TABLE IF EXISTS test_01048.wv;
CREATE WINDOW VIEW test_01048.wv AS SELECT count(a) AS count, hop(timestamp, INTERVAL '1' SECOND, INTERVAL '3' SECOND) AS wid FROM test_01048.mt GROUP BY wid;
SHOW CREATE TABLE test_01048.`.inner.wv`;

SELECT '||---IDENTIFIER---';
DROP TABLE IF EXISTS test_01048.wv;
CREATE WINDOW VIEW test_01048.wv AS SELECT count(a) AS count FROM test_01048.mt GROUP BY b, hop(timestamp, INTERVAL '1' SECOND, INTERVAL '3' SECOND) AS wid;
SHOW CREATE TABLE test_01048.`.inner.wv`;

DROP TABLE IF EXISTS test_01048.wv;
CREATE WINDOW VIEW test_01048.wv AS SELECT count(a) AS count FROM test_01048.mt GROUP BY hop(timestamp, INTERVAL '1' SECOND, INTERVAL '3' SECOND) AS wid, b;
SHOW CREATE TABLE test_01048.`.inner.wv`;

SELECT '||---FUNCTION---';
DROP TABLE IF EXISTS test_01048.wv;
CREATE WINDOW VIEW test_01048.wv AS SELECT count(a) AS count FROM test_01048.mt GROUP BY plus(a, b) as _type, hop(timestamp, INTERVAL '1' SECOND, INTERVAL '3' SECOND) AS wid;
SHOW CREATE TABLE test_01048.`.inner.wv`;

SELECT '||---TimeZone---';
DROP TABLE IF EXISTS test_01048.wv;
CREATE WINDOW VIEW test_01048.wv AS SELECT count(a) AS count, hopEnd(wid) as wend FROM test_01048.mt GROUP BY hop(timestamp, INTERVAL 1 SECOND, INTERVAL 3 SECOND, 'Asia/Shanghai') as wid;
SHOW CREATE TABLE test_01048.`.inner.wv`;


DROP TABLE IF EXISTS test_01048.wv;
CREATE WINDOW VIEW test_01048.wv AS SELECT count(a) AS count FROM test_01048.mt GROUP BY hop(timestamp, INTERVAL '1' SECOND, INTERVAL '3' SECOND) AS wid, plus(a, b);
SHOW CREATE TABLE test_01048.`.inner.wv`;

DROP TABLE test_01048.wv;
DROP TABLE test_01048.mt;
