set insert_distributed_sync=1;

DROP TABLE IF EXISTS dist_00967;
DROP TABLE IF EXISTS underlying_00967;

-- To suppress "Structure does not match (...), implicit conversion will be done." message
SET send_logs_level='error';

CREATE TABLE dist_00967 (key UInt64) Engine=Distributed('test_shard_localhost', currentDatabase(), underlying_00967);
CREATE TABLE underlying_00967 (key Nullable(UInt64)) Engine=TinyLog();
INSERT INTO dist_00967 SELECT toUInt64(number) FROM system.numbers LIMIT 1;

SELECT * FROM dist_00967;

DROP TABLE dist_00967;
DROP TABLE underlying_00967;
