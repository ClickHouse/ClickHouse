-- Tags: no-ordinary-database, no-fasttest

DROP TABLE IF EXISTS 03549_test SYNC;

CREATE TABLE 03549_test (key UInt64, value UInt64 COMMENT 'value') Engine=KeeperMap('/' || currentDatabase() || '/test3549') PRIMARY KEY(key);
CREATE TABLE 03549_test_another (key UInt64 COMMENT 'key', value UInt64) Engine=KeeperMap('/' || currentDatabase() || '/test3549') PRIMARY KEY(key);

INSERT INTO 03549_test VALUES (1, 11);
SELECT * FROM 03549_test_another ORDER BY key;
