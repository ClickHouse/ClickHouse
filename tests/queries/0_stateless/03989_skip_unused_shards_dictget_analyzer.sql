-- Tags: shard

-- force_optimize_skip_unused_shards used to fail with UNABLE_TO_SKIP_UNUSED_SHARDS
-- when the query contained GLOBAL IN with a subquery on another distributed table,
-- because filter_actions_dag was not propagated to distributed tables inside IN subqueries.

SET allow_nondeterministic_optimize_skip_unused_shards = 1;
SET optimize_skip_unused_shards = 1;
SET force_optimize_skip_unused_shards = 2;
SET check_table_dependencies = 0;
SET enable_analyzer = 1;

DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE:Identifier};
DROP TABLE IF EXISTS logs_03989;
DROP TABLE IF EXISTS server_logs_03989;
DROP TABLE IF EXISTS dist_logs_03989;
DROP TABLE IF EXISTS dist_server_logs_03989;

CREATE DATABASE {CLICKHOUSE_DATABASE:Identifier};
USE {CLICKHOUSE_DATABASE:Identifier};

CREATE TABLE logs_03989 (
    Timestamp DateTime,
    Body String,
    Region String,
    Namespace String,
    HostName String,
    value UInt64
) ENGINE = Memory;

INSERT INTO logs_03989 VALUES
    ('2024-01-01 00:00:00', 'log1', 'us-east-1', '_journald', 'host1', 1),
    ('2024-01-01 00:00:01', 'log2', 'eu-west-1', '_journald', 'host2', 2);

CREATE TABLE server_logs_03989 (
    Timestamp DateTime,
    NodeName String,
    ContainerName String,
    Namespace String,
    Region String
) ENGINE = Memory;

INSERT INTO server_logs_03989 VALUES
    ('2024-01-01 00:00:00', 'host1', 'app', 'ns-test', 'us-east-1'),
    ('2024-01-01 00:00:01', 'host2', 'app', 'ns-test', 'eu-west-1');

CREATE TABLE {CLICKHOUSE_DATABASE:Identifier}.region_to_shard (Region String, shardID UInt64) ENGINE = Memory;
INSERT INTO {CLICKHOUSE_DATABASE:Identifier}.region_to_shard VALUES ('us-east-1', 0), ('eu-west-1', 1);

CREATE DICTIONARY {CLICKHOUSE_DATABASE:Identifier}.regionToShard (Region String, shardID UInt64)
PRIMARY KEY Region
SOURCE(CLICKHOUSE(HOST '127.0.0.1' PORT tcpPort() TABLE 'region_to_shard' DB currentDatabase() USER 'default' PASSWORD ''))
LIFETIME(0)
LAYOUT(COMPLEX_KEY_HASHED());

SYSTEM RELOAD DICTIONARY {CLICKHOUSE_DATABASE:Identifier}.regionToShard;

CREATE TABLE dist_logs_03989 AS logs_03989
ENGINE = Distributed('test_cluster_two_shards', currentDatabase(), logs_03989, dictGetUInt64({CLICKHOUSE_DATABASE:String} || '.regionToShard', 'shardID', Region));

CREATE TABLE dist_server_logs_03989 AS server_logs_03989
ENGINE = Distributed('test_cluster_two_shards', currentDatabase(), server_logs_03989, dictGetUInt64({CLICKHOUSE_DATABASE:String} || '.regionToShard', 'shardID', Region));

-- Simple WHERE on sharding key column
SELECT Body FROM dist_logs_03989 WHERE Region = 'us-east-1' ORDER BY Body;

-- IN subquery on a local (non-distributed) table
SELECT Body FROM dist_logs_03989
WHERE HostName IN (SELECT NodeName FROM server_logs_03989 WHERE Region = 'us-east-1')
    AND Region = 'us-east-1'
ORDER BY Body;

-- GLOBAL IN subquery on a local (non-distributed) table
SELECT Body FROM dist_logs_03989
WHERE HostName GLOBAL IN (SELECT NodeName FROM server_logs_03989 WHERE Region = 'us-east-1')
    AND Region = 'us-east-1'
ORDER BY Body;

-- GLOBAL IN subquery on another distributed table
SELECT Body FROM dist_logs_03989
WHERE HostName GLOBAL IN (SELECT NodeName FROM dist_server_logs_03989 WHERE Region = 'us-east-1')
    AND Region = 'us-east-1'
ORDER BY Body;

DROP TABLE dist_server_logs_03989;
DROP TABLE dist_logs_03989;
DROP TABLE server_logs_03989;
DROP TABLE logs_03989;
DROP TABLE {CLICKHOUSE_DATABASE:Identifier}.region_to_shard;
DROP DICTIONARY {CLICKHOUSE_DATABASE:Identifier}.regionToShard;
DROP DATABASE {CLICKHOUSE_DATABASE:Identifier};
