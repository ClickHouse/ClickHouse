CREATE TABLE mergetree (x UInt64, s String) ENGINE = MergeTree ORDER BY x;
INSERT INTO mergetree VALUES (1, 'hello'), (2, 'world');

SELECT CASE x WHEN 1 THEN 'hello' WHEN 2 THEN 'world' ELSE  'unknown' END FROM mergetree;
SELECT count() AS cnt FROM (SELECT CASE x WHEN 1 THEN 'hello' WHEN 2 THEN 'world' ELSE  'unknown' END FROM mergetree);

CREATE TABLE distributed AS mergetree ENGINE = Distributed(test_shard_localhost, default, mergetree);

SELECT CASE x WHEN 1 THEN 'hello' WHEN 2 THEN 'world' ELSE  'unknown' END FROM distributed;
SELECT count() AS cnt FROM (SELECT CASE x WHEN 1 THEN 'hello' WHEN 2 THEN 'world' ELSE  'unknown' END FROM distributed);
