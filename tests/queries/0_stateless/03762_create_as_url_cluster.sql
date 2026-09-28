-- Tags: no-replicated-database
-- no-replicated-database: It messes up the output and this test explicitly checks the replicated database

DROP DATABASE {CLICKHOUSE_DATABASE:Identifier};
CREATE DATABASE {CLICKHOUSE_DATABASE:Identifier} ENGINE = Replicated('/clickhouse/03762_create_as_url_cluster/{database}_replicated', 'shard1', 'replica1') FORMAT Null;
CREATE TABLE {CLICKHOUSE_DATABASE:Identifier}.test (c0 Int) ENGINE = Memory AS (SELECT 1 FROM url('http://localhost:8123/?query=SELECT+1+FORMAT+Values', 'Values', 'c0 Int') tx)
