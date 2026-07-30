-- Tags: no-fasttest
-- no-fasttest: requires a build with the silk runtime

CREATE TABLE target (x UInt64) ENGINE = Memory;
CREATE TABLE runner (query String, database String) ENGINE = QueryRunner SETTINGS cluster = 'test_shard_localhost', mode = 'synchronous', scheduler = 'fibers';
INSERT INTO runner (query, database) SELECT 'INSERT INTO target VALUES (42)', currentDatabase();
SELECT * FROM target;

-- The fiber dispatcher is actually active: silk fibers ran on this server.
SYSTEM RELOAD ASYNCHRONOUS METRICS;
SELECT value > 0 FROM system.asynchronous_metrics WHERE metric = 'SilkFiberStarted';
