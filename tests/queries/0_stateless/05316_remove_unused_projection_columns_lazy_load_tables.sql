-- Tags: no-replicated-database
-- A MergeTree table of a lazy_load_tables database is read through a proxy, and the unused subquery column is still not read.

DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier};
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier} ENGINE = Atomic SETTINGS lazy_load_tables = 1;
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t (s String, id UInt8) ENGINE = MergeTree ORDER BY tuple() SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
INSERT INTO {CLICKHOUSE_DATABASE_1:Identifier}.t SELECT randomPrintableASCII(1000), number % 256 FROM numbers(1000);
DETACH DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
ATTACH DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
USE {CLICKHOUSE_DATABASE_1:Identifier};
SELECT engine FROM system.tables WHERE database = currentDatabase() AND name = 't';
SELECT count() FROM (SELECT s, id FROM {CLICKHOUSE_DATABASE_1:Identifier}.t LIMIT 100000) SETTINGS max_bytes_to_read = 100000;
DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
