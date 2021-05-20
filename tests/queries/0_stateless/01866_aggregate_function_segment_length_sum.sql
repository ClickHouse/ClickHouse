DROP TABLE IF EXISTS segment;
DROP TABLE IF EXISTS fl_segment;
DROP TABLE IF EXISTS dt_segment;
DROP TABLE IF EXISTS date_segment;

CREATE TABLE segment ( `id` String, `start` Int64, `end` Int64 ) ENGINE = MergeTree ORDER BY start;
INSERT INTO segment VALUES ('a', 1, 3), ('a', 1, 3), ('a', 2, 4), ('a', 1, 1), ('a', 5, 6), ('a', 5, 7), ('b', 10, 12), ('b', 13, 19), ('b', 14, 16), ('c', -1, 1), ('c', -2, -1);

CREATE TABLE fl_segment ( `id` String, `start` Float, `end` Float ) ENGINE = MergeTree ORDER BY start;
INSERT INTO fl_segment VALUES ('a', 1.1, 3.2), ('a', 1.5, 3.6), ('a', 4.0, 5.0);

CREATE TABLE dt_segment ( `id` String, `start` DateTime, `end` DateTime ) ENGINE = MergeTree ORDER BY start;
INSERT INTO dt_segment VALUES ('a', '2020-01-01 02:11:22', '2020-01-01 03:12:31'), ('a', '2020-01-01 01:12:30', '2020-01-01 02:50:11');

CREATE TABLE date_segment ( `id` String, `start` Date, `end` Date ) ENGINE = MergeTree ORDER BY start;
INSERT INTO date_segment VALUES ('a', '2020-01-01', '2020-01-04'), ('a', '2020-01-03', '2020-01-08 02:50:11');

SELECT id, segmentLengthSum(start, end), toTypeName(segmentLengthSum(start, end)) FROM segment GROUP BY id ORDER BY id;
SELECT id, 3.4 < segmentLengthSum(start, end) AND segmentLengthSum(start, end) < 3.6, toTypeName(segmentLengthSum(start, end)) FROM fl_segment GROUP BY id ORDER BY id;
SELECT id, segmentLengthSum(start, end), toTypeName(segmentLengthSum(start, end)) FROM dt_segment GROUP BY id ORDER BY id;
SELECT id, segmentLengthSum(start, end), toTypeName(segmentLengthSum(start, end)) FROM date_segment GROUP BY id ORDER BY id;

DROP TABLE segment;
DROP TABLE fl_segment;
DROP TABLE dt_segment;
DROP TABLE date_segment;
