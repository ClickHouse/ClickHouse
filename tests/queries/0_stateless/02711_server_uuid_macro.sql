DROP TABLE IF EXISTS test;

-- You can create a table with the {server_uuid} substituted.
CREATE TABLE test (x UInt8) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/test/{shard}', 'replica-{server_uuid}') ORDER BY x;

-- The server UUID is correctly substituted.
SELECT engine_full LIKE ('%replica-{server_uuid}%') FROM system.tables WHERE database = currentDatabase() AND name = 'test';
SELECT count() > 0 FROM system.zookeeper WHERE path = '/clickhouse/tables/' || currentDatabase() || '/test/s1/replicas/' AND name LIKE 'replica-' || serverUUID()::String || '%';

-- An attempt to create a second table with the same UUID results in error.
CREATE TABLE test2 (x UInt8) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/test/{shard}', 'replica-{server_uuid}') ORDER BY x; -- { serverError REPLICA_ALREADY_EXISTS }

-- The macro {server_uuid} is special, not a configuration-type macro. It's normal that it is inaccessible with the getMacro function.
SELECT getMacro('server_uuid'); -- { serverError NO_ELEMENTS_IN_CONFIG }

DROP TABLE test SYNC;
