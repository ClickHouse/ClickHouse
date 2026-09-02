-- Tags: no-ordinary-database, no-fasttest

DROP TABLE IF EXISTS 02417_test SYNC;

CREATE TABLE 02417_test (key UInt64, value UInt64) Engine=KeeperMap('/' || currentDatabase() || '/test2417') PRIMARY KEY(key);
INSERT INTO 02417_test VALUES (1, 11);
SELECT * FROM 02417_test ORDER BY key;
SELECT '------';

CREATE TABLE 02417_test_another (key UInt64, value UInt64) Engine=KeeperMap('/' || currentDatabase() || '/test2417') PRIMARY KEY(key);
INSERT INTO 02417_test_another VALUES (2, 22);
SELECT * FROM 02417_test_another ORDER BY key;
SELECT '------';
SELECT * FROM 02417_test ORDER BY key;
SELECT '------';

DROP TABLE 02417_test SYNC;
SELECT * FROM 02417_test_another ORDER BY key;

DROP TABLE 02417_test_another SYNC;
