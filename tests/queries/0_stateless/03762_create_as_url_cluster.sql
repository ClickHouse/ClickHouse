-- Tags: no-replicated-database
-- no-replicated-database: It messes up the output and this test explicitly checks the replicated database

-- {CLICKHOUSE_DATABASE} must not be re-engined: with --database it is shared by every test in
-- the client. {CLICKHOUSE_DATABASE_2} is shared too, so both DROPs are required.
DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_2:Identifier};
CREATE DATABASE {CLICKHOUSE_DATABASE_2:Identifier} ENGINE = Replicated('/clickhouse/03762_create_as_url_cluster/{database}_replicated', 'shard1', 'replica1') FORMAT Null;
CREATE TABLE {CLICKHOUSE_DATABASE_2:Identifier}.test (c0 Int) ENGINE = Memory AS (SELECT 1 FROM url('http://localhost:8123/?query=SELECT+1+FORMAT+Values', 'Values', 'c0 Int') tx);
DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_2:Identifier};
