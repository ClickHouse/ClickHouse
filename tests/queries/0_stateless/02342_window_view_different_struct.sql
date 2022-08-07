SET allow_experimental_window_view = 1;

DROP TABLE IF EXISTS window_view_02342;
DROP TABLE IF EXISTS data_02342;

-- ALTER
CREATE TABLE data_02342 (a UInt8) ENGINE=MergeTree ORDER BY a;
CREATE WINDOW VIEW window_view_02342 ENGINE=Memory AS SELECT count(a), tumbleStart(wid) AS w_start, tumbleEnd(tumble(now(), INTERVAL '3' SECOND)) AS w_end FROM data_02342 GROUP BY tumble(now(), INTERVAL '3' SECOND) AS wid;
INSERT INTO data_02342 VALUES (42);
ALTER TABLE data_02342 ADD COLUMN s String;
INSERT INTO data_02342 VALUES (42, 'data_02342');
DROP TABLE window_view_02342;
DROP TABLE data_02342;

-- DROP/CREATE
CREATE TABLE data_02342 (a UInt8) ENGINE=MergeTree ORDER BY a;
CREATE WINDOW VIEW window_view_02342 ENGINE=Memory AS SELECT count(a), tumbleStart(wid) AS w_start, tumbleEnd(tumble(now(), INTERVAL '3' SECOND)) AS w_end FROM data_02342 GROUP BY tumble(now(), INTERVAL '3' SECOND) AS wid;
INSERT INTO data_02342 VALUES (42);
DROP TABLE window_view_02342;
DROP TABLE data_02342;

CREATE TABLE data_02342 (a UInt8, s String) ENGINE=MergeTree ORDER BY a;
CREATE WINDOW VIEW window_view_02342 ENGINE=Memory AS SELECT count(a), tumbleStart(wid) AS w_start, tumbleEnd(tumble(now(), INTERVAL '3' SECOND)) AS w_end FROM data_02342 GROUP BY tumble(now(), INTERVAL '3' SECOND) AS wid;
INSERT INTO data_02342 VALUES (42, 'data_02342');
DROP TABLE window_view_02342;
DROP TABLE data_02342;
