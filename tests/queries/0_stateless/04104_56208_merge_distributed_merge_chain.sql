-- Tags: distributed
--
-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/56208.

DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier};
DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_2:Identifier};
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
CREATE DATABASE {CLICKHOUSE_DATABASE_2:Identifier};

CREATE TABLE {CLICKHOUSE_DATABASE_2:Identifier}.local_data (name String) ENGINE = MergeTree ORDER BY name AS SELECT toString(number) FROM numbers(10);

CREATE TABLE {CLICKHOUSE_DATABASE_2:Identifier}.local_merged_data (name String) ENGINE = Merge({CLICKHOUSE_DATABASE_2:Identifier}, '^local_data$');

CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.distributed_data (name String) ENGINE = Distributed(test_shard_localhost, {CLICKHOUSE_DATABASE_2:Identifier}, local_merged_data);

CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.local_merged_data (name String) ENGINE = Merge({CLICKHOUSE_DATABASE_1:Identifier}, '^distributed_data$');

-- `prefer_localhost_replica = 0` forces the Distributed query to go through the network layer
-- (and through the remote-parse path that was broken), even though the shard is on localhost.
SET prefer_localhost_replica = 0;

SELECT '-- enable_analyzer=1 --';
SET enable_analyzer = 1;
SELECT count() FROM {CLICKHOUSE_DATABASE_1:Identifier}.local_merged_data;
SELECT DISTINCT _table FROM {CLICKHOUSE_DATABASE_1:Identifier}.local_merged_data;

SELECT '-- enable_analyzer=0 --';
SET enable_analyzer = 0;
SELECT count() FROM {CLICKHOUSE_DATABASE_1:Identifier}.local_merged_data;
SELECT DISTINCT _table FROM {CLICKHOUSE_DATABASE_1:Identifier}.local_merged_data;

DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
DROP DATABASE {CLICKHOUSE_DATABASE_2:Identifier};
