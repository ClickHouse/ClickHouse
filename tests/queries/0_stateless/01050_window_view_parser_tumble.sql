SET allow_experimental_window_view = 1;

DROP TABLE IF EXISTS mt;

CREATE TABLE mt(a Int32, timestamp DateTime) ENGINE=MergeTree ORDER BY tuple();

SELECT '---WATERMARK---';
DROP TABLE IF EXISTS wv NO DELAY;
CREATE WINDOW VIEW wv ENGINE Memory WATERMARK=INTERVAL '1' SECOND AS SELECT count(a), tumbleStart(wid) AS w_start, tumbleEnd(wid) AS w_end FROM mt GROUP BY tumble(timestamp, INTERVAL '3' SECOND) AS wid;

SELECT '---With w_end---';
DROP TABLE IF EXISTS wv NO DELAY;
CREATE WINDOW VIEW wv ENGINE Memory AS SELECT count(a), tumbleStart(tumble(timestamp, INTERVAL '3' SECOND)) AS w_start, tumbleEnd(wid) AS w_end FROM mt GROUP BY tumble(timestamp, INTERVAL '3' SECOND) AS wid;

SELECT '---WithOut w_end---';
DROP TABLE IF EXISTS wv NO DELAY;
CREATE WINDOW VIEW wv ENGINE Memory AS SELECT count(a), tumbleStart(wid) AS w_start FROM mt GROUP BY tumble(timestamp, INTERVAL '3' SECOND) AS wid;

SELECT '---WITH---';
DROP TABLE IF EXISTS wv NO DELAY;
CREATE WINDOW VIEW wv ENGINE Memory AS WITH toDateTime('2018-01-01 00:00:00') AS date_time SELECT count(a), tumbleStart(wid) AS w_start, tumbleEnd(wid) AS w_end, date_time FROM mt GROUP BY tumble(timestamp, INTERVAL '3' SECOND) AS wid;

SELECT '---WHERE---';
DROP TABLE IF EXISTS wv NO DELAY;
CREATE WINDOW VIEW wv ENGINE Memory AS SELECT count(a), tumbleStart(wid) AS w_start FROM mt WHERE a != 1 GROUP BY tumble(timestamp, INTERVAL '3' SECOND) AS wid;

SELECT '---ORDER_BY---';
DROP TABLE IF EXISTS wv NO DELAY;
CREATE WINDOW VIEW wv ENGINE Memory AS SELECT count(a), tumbleStart(wid) AS w_start FROM mt WHERE a != 1 GROUP BY tumble(timestamp, INTERVAL '3' SECOND) AS wid ORDER BY w_start;

SELECT '---With now---';
DROP TABLE IF EXISTS wv NO DELAY;
CREATE WINDOW VIEW wv ENGINE Memory AS SELECT count(a), tumbleStart(wid) AS w_start, tumbleEnd(tumble(now(), INTERVAL '3' SECOND)) AS w_end FROM mt GROUP BY tumble(now(), INTERVAL '3' SECOND) AS wid;
