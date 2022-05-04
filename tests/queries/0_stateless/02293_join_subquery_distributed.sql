-- Tags: distributed, no-parallel

DROP DATABASE IF EXISTS 02293_join_subquery_distributed;
CREATE DATABASE 02293_join_subquery_distributed;
USE 02293_join_subquery_distributed;

DROP TABLE IF EXISTS test_distributed;
DROP TABLE IF EXISTS test_local;

SET prefer_localhost_replica = 0;

CREATE TABLE test_local (text String, text2 String) ENGINE = MergeTree() ORDER BY text;
CREATE TABLE test_distributed (text String, text2 String) ENGINE = Distributed('test_shard_localhost', currentDatabase(), test_local);
INSERT INTO test_distributed VALUES ('a', 'b');

SET joined_subquery_requires_alias = 0;

SELECT COUNT() AS count FROM test_distributed INNER JOIN ( SELECT text FROM test_distributed WHERE text == 'a') USING (text);

DROP TABLE IF EXISTS test_distributed;
DROP TABLE IF EXISTS test_local;

DROP DATABASE IF EXISTS 02293_join_subquery_distributed;
