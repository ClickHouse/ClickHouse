DROP DATABASE IF EXISTS test_show_limit;

CREATE DATABASE test_show_limit;

CREATE TABLE test_show_limit.test1 (test UInt8) ENGINE = TinyLog;
CREATE TABLE test_show_limit.test2 (test UInt8) ENGINE = TinyLog;
CREATE TABLE test_show_limit.test3 (test UInt8) ENGINE = TinyLog;
CREATE TABLE test_show_limit.test4 (test UInt8) ENGINE = TinyLog;
CREATE TABLE test_show_limit.test5 (test UInt8) ENGINE = TinyLog;
CREATE TABLE test_show_limit.test6 (test UInt8) ENGINE = TinyLog;

SELECT '*** Should show 6: ***';
SHOW TABLES FROM test_show_limit;
SELECT '*** Should show 2: ***';
SHOW TABLES FROM test_show_limit LIMIT 2;
SELECT '*** Should show 4: ***';
SHOW TABLES FROM test_show_limit LIMIT 2 * 2;

DROP DATABASE test_show_limit;

