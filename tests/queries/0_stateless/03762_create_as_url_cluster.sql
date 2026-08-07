-- Tags: no-replicated-database
-- no-replicated-database: It messes up the output and this test explicitly checks the replicated database

-- Use a scratch database instead of re-engining {CLICKHOUSE_DATABASE}: in shared-database mode
-- (stress lane) that database is shared by every test in the client and is never recreated, so
-- leaving it Replicated breaks later tests that edit on-disk metadata. The leading DROP makes this
-- test immune to a predecessor's leftover, the trailing one stops it becoming that predecessor.
DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier};
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier} ENGINE = Replicated('/clickhouse/03762_create_as_url_cluster/{database}_replicated', 'shard1', 'replica1') FORMAT Null;
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.test (c0 Int) ENGINE = Memory AS (SELECT 1 FROM url('http://localhost:8123/?query=SELECT+1+FORMAT+Values', 'Values', 'c0 Int') tx);
DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier};
