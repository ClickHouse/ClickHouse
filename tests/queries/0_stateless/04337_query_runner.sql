CREATE TABLE runner (query String, database String, settings Map(String, String)) ENGINE = QueryRunner SETTINGS mode = 'synchronous';

CREATE TABLE bad (q String) ENGINE = QueryRunner; -- { serverError BAD_ARGUMENTS }
CREATE TABLE bad (database String) ENGINE = QueryRunner; -- { serverError BAD_ARGUMENTS }
CREATE TABLE bad (query UInt64) ENGINE = QueryRunner; -- { serverError BAD_ARGUMENTS }
CREATE TABLE bad (query String, settings Map(String, UInt64)) ENGINE = QueryRunner; -- { serverError BAD_ARGUMENTS }
CREATE TABLE bad (query String, delay_microseconds UInt64) ENGINE = QueryRunner; -- { serverError BAD_ARGUMENTS }
CREATE TABLE bad (query String, database String ALIAS query) ENGINE = QueryRunner; -- { serverError BAD_ARGUMENTS }
CREATE TABLE bad (query String) ENGINE = QueryRunner('arg'); -- { serverError BAD_ARGUMENTS }
CREATE TABLE bad (query String) ENGINE = QueryRunner SETTINGS mode = 'sometimes'; -- { serverError BAD_ARGUMENTS }
CREATE TABLE bad (query String) ENGINE = QueryRunner SETTINGS threads = 0; -- { serverError BAD_ARGUMENTS }
CREATE TABLE bad (query String) ENGINE = QueryRunner SETTINGS max_queue_size = 0; -- { serverError BAD_ARGUMENTS }
CREATE TABLE bad (query String) ENGINE = QueryRunner SETTINGS cluster = 'no_such_cluster'; -- { serverError CLUSTER_DOESNT_EXIST }
CREATE TABLE bad (query String) ENGINE = QueryRunner SETTINGS unknown_setting = 1; -- { serverError UNKNOWN_SETTING }
CREATE TABLE bad (query String) ENGINE = QueryRunner SETTINGS cluster = 'test_shard_localhost' SQL SECURITY NONE; -- { serverError BAD_ARGUMENTS }
CREATE TABLE bad (query String) ENGINE = QueryRunner SETTINGS cluster = 'test_shard_localhost' DEFINER = default SQL SECURITY DEFINER; -- { serverError BAD_ARGUMENTS }
CREATE TABLE bad (query String) ENGINE = QueryRunner SETTINGS shard = '2'; -- { serverError BAD_ARGUMENTS }
CREATE TABLE bad (query String) ENGINE = QueryRunner SETTINGS cluster = 'test_shard_localhost', shard = '5'; -- { serverError BAD_ARGUMENTS }
CREATE TABLE bad (query String) ENGINE = QueryRunner SETTINGS cluster = 'test_shard_localhost', shard = 'nope'; -- { serverError BAD_ARGUMENTS }
CREATE TABLE bad (query String) ENGINE = QueryRunner SETTINGS shard = 'all'; -- { serverError BAD_ARGUMENTS }
CREATE TABLE bad (query LowCardinality(String)) ENGINE = QueryRunner; -- { serverError BAD_ARGUMENTS }

SELECT * FROM runner; -- { serverError NOT_IMPLEMENTED }
ALTER TABLE runner ADD COLUMN extra String; -- { serverError NOT_IMPLEMENTED }
ALTER TABLE runner DROP COLUMN database; -- { serverError NOT_IMPLEMENTED }
ALTER TABLE runner MODIFY SQL SECURITY INVOKER; -- { serverError NOT_IMPLEMENTED }

CREATE TABLE runner2 (query String) ENGINE = QueryRunner SETTINGS mode = 'synchronous', threads = 2, max_queue_size = 100 SQL SECURITY INVOKER;
SHOW CREATE TABLE runner2;

CREATE TABLE runner_random (query String) ENGINE = QueryRunner SETTINGS cluster = 'test_shard_localhost', shard = 'random';
CREATE TABLE runner_all (query String) ENGINE = QueryRunner SETTINGS cluster = 'test_shard_localhost', shard = 'all';

SYSTEM WAIT QUERY RUNNER runner;
SYSTEM WAIT QUERY RUNNER runner2;
SYSTEM WAIT QUERY RUNNER runner_random;
SYSTEM WAIT QUERY RUNNER runner_all;
