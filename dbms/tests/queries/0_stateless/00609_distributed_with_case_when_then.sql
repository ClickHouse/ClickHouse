DROP TABLE IF EXISTS mergetree;
DROP TABLE IF EXISTS distributed;

CREATE TABLE mergetree (x UInt64, s String) ENGINE = MergeTree ORDER BY x;
INSERT INTO mergetree VALUES (1, 'hello'), (2, 'world');

SELECT CASE x WHEN 1 THEN 'hello' WHEN 2 THEN 'world' ELSE  'unknow' END FROM mergetree;
SELECT count() AS cnt FROM (SELECT CASE x WHEN 1 THEN 'hello' WHEN 2 THEN 'world' ELSE  'unknow' END FROM mergetree);

CREATE TABLE distributed AS mergetree ENGINE = Distributed(test_shard_localhost, test, mergetree);

SELECT CASE x WHEN 1 THEN 'hello' WHEN 2 THEN 'world' ELSE  'unknow' END FROM distributed;
SELECT count() AS cnt FROM (SELECT CASE x WHEN 1 THEN 'hello' WHEN 2 THEN 'world' ELSE  'unknow' END FROM distributed);

DROP TABLE mergetree;
DROP TABLE distributed;
