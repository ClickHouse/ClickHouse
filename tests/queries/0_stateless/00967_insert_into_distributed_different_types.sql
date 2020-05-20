DROP TABLE IF EXISTS dist_00967;
DROP TABLE IF EXISTS underlying_00967;

CREATE TABLE dist_00967 (key UInt64) Engine=Distributed('test_shard_localhost', currentDatabase(), underlying_00967);
-- fails for TinyLog()/MergeTree()/... but not for Memory()
CREATE TABLE underlying_00967 (key Nullable(UInt64)) Engine=TinyLog();
INSERT INTO dist_00967 SELECT toUInt64(number) FROM system.numbers LIMIT 1;

SELECT * FROM dist_00967;
