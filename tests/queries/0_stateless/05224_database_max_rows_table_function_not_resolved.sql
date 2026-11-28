-- Tags: no-fasttest
-- The `max_rows` accounting on CREATE/ATTACH must not resolve a table function: `some_addr` does not exist,
-- so resolving `url` here would fail with DNS_ERROR. A table function holds no MergeTree data, so it counts as 0 rows.

DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE:Identifier};
CREATE DATABASE {CLICKHOUSE_DATABASE:Identifier} ENGINE = Atomic SETTINGS max_rows = 10;

CREATE TABLE {CLICKHOUSE_DATABASE:Identifier}.t (x UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO {CLICKHOUSE_DATABASE:Identifier}.t SELECT number FROM numbers(10);

CREATE TABLE {CLICKHOUSE_DATABASE:Identifier}.tf (x String) AS url('http://some_addr:9000/nonexistent');
DETACH TABLE {CLICKHOUSE_DATABASE:Identifier}.tf;
ATTACH TABLE {CLICKHOUSE_DATABASE:Identifier}.tf;

SELECT name, engine FROM system.tables WHERE database = currentDatabase() ORDER BY name;
SELECT rows FROM system.databases WHERE name = currentDatabase();

DROP DATABASE {CLICKHOUSE_DATABASE:Identifier};
