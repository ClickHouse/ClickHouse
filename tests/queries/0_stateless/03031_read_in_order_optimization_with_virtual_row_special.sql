-- Tags: no-parallel

-- modified from test_01155_ordinary
DROP DATABASE IF EXISTS test_01155_ordinary;

SET allow_deprecated_database_ordinary = 1;

CREATE DATABASE test_01155_ordinary ENGINE = Ordinary;

USE test_01155_ordinary;

CREATE TABLE src (s String) ENGINE = MergeTree() ORDER BY s;
INSERT INTO src(s) VALUES ('before moving tables');
CREATE TABLE dist (s String) ENGINE = Distributed(test_shard_localhost, test_01155_ordinary, src);

SET enable_analyzer=0;
SELECT _table FROM merge('test_01155_ordinary', '') ORDER BY _table, s;

DROP TABLE src;
DROP TABLE dist;
DROP DATABASE test_01155_ordinary;