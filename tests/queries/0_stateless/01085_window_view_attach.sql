-- Tags: no-parallel

SET allow_experimental_window_view = 1;

DROP DATABASE IF EXISTS test_01085;
CREATE DATABASE test_01085 ENGINE=Ordinary;

DROP TABLE IF EXISTS test_01085.mt;
DROP TABLE IF EXISTS test_01085.wv;

CREATE TABLE test_01085.mt(a Int32, market Int32, timestamp DateTime) ENGINE=MergeTree ORDER BY tuple();
CREATE WINDOW VIEW test_01085.wv ENGINE Memory WATERMARK=ASCENDING AS SELECT count(a) AS count, market, tumbleEnd(wid) AS w_end FROM test_01085.mt GROUP BY tumble(timestamp, INTERVAL '5' SECOND) AS wid, market;

SHOW tables FROM test_01085;

DROP TABLE test_01085.wv NO DELAY;
SHOW tables FROM test_01085;

CREATE WINDOW VIEW test_01085.wv ENGINE Memory WATERMARK=ASCENDING AS SELECT count(a) AS count, market, tumbleEnd(wid) AS w_end FROM test_01085.mt GROUP BY tumble(timestamp, INTERVAL '5' SECOND) AS wid, market;

DETACH TABLE test_01085.wv;
SHOW tables FROM test_01085;

ATTACH TABLE test_01085.wv;
SHOW tables FROM test_01085;

DROP TABLE test_01085.wv NO DELAY;
SHOW tables FROM test_01085;
