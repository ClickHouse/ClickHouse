CREATE TABLE tuple_01015(a Tuple(DateTime, Int32)) ENGINE = MergeTree() ORDER BY a;
-- repeat a couple of times, because it doesn't always reproduce well
INSERT INTO tuple_01015 VALUES (('2018-01-01 00:00:00', 1));
SELECT * FROM tuple_01015 WHERE a < tuple(toDateTime('2019-01-01 00:00:00'), 0) format Null;
INSERT INTO tuple_01015 VALUES (('2018-01-01 00:00:00', 1));
SELECT * FROM tuple_01015 WHERE a < tuple(toDateTime('2019-01-01 00:00:00'), 0) format Null;
INSERT INTO tuple_01015 VALUES (('2018-01-01 00:00:00', 1));
SELECT * FROM tuple_01015 WHERE a < tuple(toDateTime('2019-01-01 00:00:00'), 0) format Null;
INSERT INTO tuple_01015 VALUES (('2018-01-01 00:00:00', 1));
SELECT * FROM tuple_01015 WHERE a < tuple(toDateTime('2019-01-01 00:00:00'), 0) format Null;
INSERT INTO tuple_01015 VALUES (('2018-01-01 00:00:00', 1));
SELECT * FROM tuple_01015 WHERE a < tuple(toDateTime('2019-01-01 00:00:00'), 0) format Null;
