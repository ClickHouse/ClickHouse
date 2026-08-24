-- Tags: shard

DROP TABLE IF EXISTS remote_test;
set allow_deprecated_syntax_for_merge_tree=1;
CREATE TABLE remote_test(uid String, its UInt32,  action_code String, day Date) ENGINE = MergeTree(day, (uid, its), 8192);
INSERT INTO remote_test SELECT toString(number) AS uid, number % 3 AS its, toString(number % 3) AS action_code, '2000-01-01' FROM system.numbers LIMIT 10000;
SELECT level, COUNT() FROM (SELECT uid, windowFunnel(3600)(toUInt32(its), action_code != '', action_code = '2') AS level FROM remote('127.0.0.{2,3}', currentDatabase(), remote_test) GROUP BY uid) GROUP BY level;
SELECT level, COUNT() FROM (SELECT uid, windowFunnel(3600)(toUInt32(its), action_code != '', action_code = '2') AS level FROM remote('127.0.0.{2,3}', currentDatabase(), remote_test) GROUP BY uid) GROUP BY level;
SELECT level, COUNT() FROM (SELECT uid, windowFunnel(3600)(toUInt32(its), action_code != '', action_code = '2') AS level FROM remote('127.0.0.{2,3}', currentDatabase(), remote_test) GROUP BY uid) GROUP BY level;
DROP TABLE IF EXISTS remote_test;
