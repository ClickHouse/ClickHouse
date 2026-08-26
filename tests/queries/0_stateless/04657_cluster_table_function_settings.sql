-- Tags: shard, no-fasttest

SET send_logs_level = 'fatal';

-- The `remote`, `cluster` and `clusterAllReplicas` table functions accept a SETTINGS clause
-- with the settings of the Distributed storage, e.g. `skip_unavailable_shards`.

SELECT count() FROM clusterAllReplicas('test_unavailable_shard', system.one, SETTINGS skip_unavailable_shards = 1);
SELECT count() FROM cluster('test_unavailable_shard', system.one, SETTINGS skip_unavailable_shards = 1);
SELECT count() FROM remote('127.0.0.2|localhost:1', system.one, SETTINGS skip_unavailable_shards = 1);

-- A setting specified in the query has priority over the setting from the table function.
SELECT count() FROM clusterAllReplicas('test_unavailable_shard', system.one, SETTINGS skip_unavailable_shards = 1) SETTINGS skip_unavailable_shards = 0; -- { serverError ALL_CONNECTION_TRIES_FAILED }

-- Only the settings of the Distributed storage are accepted.
SELECT count() FROM clusterAllReplicas('test_shard_localhost', system.one, SETTINGS no_such_setting = 1); -- { serverError UNKNOWN_SETTING }
SELECT count() FROM clusterAllReplicas('test_shard_localhost', system.one, SETTINGS max_threads = 1); -- { serverError UNKNOWN_SETTING }

-- The SETTINGS clause is preserved in the definition of a table created over the table function.
DROP TABLE IF EXISTS table_over_cluster_function;
CREATE TABLE table_over_cluster_function AS clusterAllReplicas('test_unavailable_shard', system.one, SETTINGS skip_unavailable_shards = 1);
SHOW CREATE TABLE table_over_cluster_function;

SELECT count() FROM table_over_cluster_function;
SELECT count() FROM table_over_cluster_function SETTINGS skip_unavailable_shards = 0; -- { serverError ALL_CONNECTION_TRIES_FAILED }

DETACH TABLE table_over_cluster_function;
ATTACH TABLE table_over_cluster_function;

SHOW CREATE TABLE table_over_cluster_function;
SELECT count() FROM table_over_cluster_function;

DROP TABLE table_over_cluster_function;
