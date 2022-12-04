-- Tags: no-ordinary-database, no-fasttest

DROP TABLE IF EXISTS _02418_test SYNC;

CREATE TABLE _02418_test (key UInt64, value Float64) Engine=KeeperMap('/' || currentDatabase() || '/test2418', 3) PRIMARY KEY(key);

INSERT INTO _02418_test VALUES (1, 1.1), (2, 2.2);
SELECT count() FROM _02418_test;

INSERT INTO _02418_test VALUES (3, 3.3), (4, 4.4); -- { serverError 290 }

INSERT INTO _02418_test VALUES (1, 2.1), (2, 3.2), (3, 3.3);
SELECT count() FROM _02418_test;

CREATE TABLE _02418_test_another (key UInt64, value Float64) Engine=KeeperMap('/' || currentDatabase() || '/test2418', 4) PRIMARY KEY(key);
INSERT INTO _02418_test VALUES (4, 4.4); -- { serverError 290 }
INSERT INTO _02418_test_another VALUES (4, 4.4);

SELECT count() FROM _02418_test;
SELECT count() FROM _02418_test_another;

DROP TABLE _02418_test SYNC;
DROP TABLE _02418_test_another SYNC;
