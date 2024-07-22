DROP TABLE IF EXISTS 03208_keepermap_test SYNC;

CREATE TABLE 03208_keepermap_test (key UInt64, value UInt64) Engine=KeeperMap('/' || currentDatabase() || '/test03208') PRIMARY KEY(key);
INSERT INTO 03208_keepermap_test VALUES (1, 11);
SYSTEM ENABLE FAILPOINT keepermap_fail_drop_data;
DROP TABLE 03208_keepermap_test SYNC; -- { KEEPER_EXCEPTION }
SYSTEM DISABLE FAILPOINT keepermap_fail_drop_data;
CREATE TABLE 03208_keepermap_test_another (key UInt64, value UInt64) Engine=KeeperMap('/' || currentDatabase() || '/test03208') PRIMARY KEY(key);
