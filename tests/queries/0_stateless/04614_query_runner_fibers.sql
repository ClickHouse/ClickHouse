-- Tags: no-fasttest
-- no-fasttest: requires a build with the silk runtime

CREATE TABLE target (x UInt64) ENGINE = Memory;
CREATE TABLE runner (query String, database String) ENGINE = QueryRunner SETTINGS cluster = 'test_shard_localhost', mode = 'synchronous', scheduler = 'fibers';
INSERT INTO runner (query, database) SELECT 'INSERT INTO target VALUES (42)', currentDatabase();
SELECT * FROM target;

-- The fiber dispatcher is actually active: silk fibers ran on this server.
SYSTEM RELOAD ASYNCHRONOUS METRICS;
SELECT value > 0 FROM system.asynchronous_metrics WHERE metric = 'SilkFiberStarted';

CREATE TABLE runner_skip (query String, database String, settings Map(String, String)) ENGINE = QueryRunner SETTINGS cluster = 'test_shard_localhost', mode = 'synchronous', scheduler = 'fibers';
CREATE TEMPORARY TABLE run AS SELECT toString(generateUUIDv4()) AS id;
CREATE TEMPORARY TABLE skipped_before AS SELECT sum(value) AS value FROM system.events WHERE event = 'DistributedShardsSkipped';
INSERT INTO runner_skip SETTINGS skip_unavailable_shards = 0
    SELECT 'SELECT * FROM no_such_table', currentDatabase(), map('log_comment', 'runner_fail_' || (SELECT id FROM run));
INSERT INTO runner_skip SETTINGS skip_unavailable_shards = 1, skip_unavailable_shards_mode = 'unavailable_or_table_missing'
    SELECT 'SELECT * FROM no_such_table', currentDatabase(), map('log_comment', 'runner_skip_table_missing_' || (SELECT id FROM run));
INSERT INTO runner_skip SETTINGS skip_unavailable_shards = 1, skip_unavailable_shards_mode = 'unavailable'
    SELECT 'SELECT * FROM no_such_table', currentDatabase(), map('log_comment', 'runner_skip_unavailable_only_' || (SELECT id FROM run));
INSERT INTO runner_skip SETTINGS skip_unavailable_shards = 1, skip_unavailable_shards_mode = 'unavailable_or_exception_before_processing'
    SELECT 'SELECT * FROM no_such_table', currentDatabase(), map('log_comment', 'runner_skip_exception_before_processing_' || (SELECT id FROM run));
SYSTEM FLUSH LOGS query_log;
SELECT replaceOne(log_comment, '_' || (SELECT id FROM run), '') AS job, type, exception_code FROM system.query_log
WHERE event_date >= yesterday() AND current_database = currentDatabase() AND endsWith(log_comment, '_' || (SELECT id FROM run)) AND is_internal AND type != 'QueryStart'
ORDER BY job;
SELECT (SELECT sum(value) FROM system.events WHERE event = 'DistributedShardsSkipped') - (SELECT value FROM skipped_before) >= 2;
DROP TABLE runner_skip;
