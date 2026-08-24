DROP TABLE IF EXISTS interval;
DROP TABLE IF EXISTS fl_interval;
DROP TABLE IF EXISTS dt_interval;
DROP TABLE IF EXISTS date_interval;

CREATE TABLE interval ( `id` String, `start` Int64, `end` Int64 ) ENGINE = MergeTree ORDER BY start;
INSERT INTO interval VALUES ('a', 1, 3), ('a', 1, 3), ('a', 2, 4), ('a', 1, 1), ('a', 5, 6), ('a', 5, 7), ('b', 10, 12), ('b', 13, 19), ('b', 14, 16), ('c', -1, 1), ('c', -2, -1);

CREATE TABLE fl_interval ( `id` String, `start` Float, `end` Float ) ENGINE = MergeTree ORDER BY start;
INSERT INTO fl_interval VALUES ('a', 1.1, 3.2), ('a', 1.5, 3.6), ('a', 4.0, 5.0);

CREATE TABLE dt_interval ( `id` String, `start` DateTime, `end` DateTime ) ENGINE = MergeTree ORDER BY start;
INSERT INTO dt_interval VALUES ('a', '2020-01-01 02:11:22', '2020-01-01 03:12:31'), ('a', '2020-01-01 01:12:30', '2020-01-01 02:50:11');

CREATE TABLE date_interval ( `id` String, `start` Date, `end` Date ) ENGINE = MergeTree ORDER BY start;
INSERT INTO date_interval VALUES ('a', '2020-01-01', '2020-01-04'), ('a', '2020-01-03', '2020-01-08 02:50:11');

SELECT id, intervalLengthSum(start, end), toTypeName(intervalLengthSum(start, end)) FROM interval GROUP BY id ORDER BY id;
SELECT id, 3.4 < intervalLengthSum(start, end) AND intervalLengthSum(start, end) < 3.6, toTypeName(intervalLengthSum(start, end)) FROM fl_interval GROUP BY id ORDER BY id;
SELECT id, intervalLengthSum(start, end), toTypeName(intervalLengthSum(start, end)) FROM dt_interval GROUP BY id ORDER BY id;
SELECT id, intervalLengthSum(start, end), toTypeName(intervalLengthSum(start, end)) FROM date_interval GROUP BY id ORDER BY id;

DROP TABLE interval;
DROP TABLE fl_interval;
DROP TABLE dt_interval;
DROP TABLE date_interval;
