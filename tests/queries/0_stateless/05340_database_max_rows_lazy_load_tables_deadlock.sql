-- Loading a lazy table whose `CHECK` constraint reads another table of its database looks up that database, so
-- neither `system.databases.rows` nor a cross-database `RENAME` may load it under the database mutex.
-- The fuzzer pin keeps this file's DDL from being replayed under another name.
SET ast_fuzzer_any_query = 0;

DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_2:Identifier};
DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier};
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier} ENGINE = Atomic SETTINGS lazy_load_tables = 1;
CREATE DATABASE {CLICKHOUSE_DATABASE_2:Identifier} ENGINE = Atomic SETTINGS max_rows = 3;

CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.source (y UInt64) ENGINE = MergeTree ORDER BY y;
INSERT INTO {CLICKHOUSE_DATABASE_1:Identifier}.source SELECT number FROM numbers(10);
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t (x UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO {CLICKHOUSE_DATABASE_1:Identifier}.t SELECT number FROM numbers(5);
ALTER TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t ADD CONSTRAINT c CHECK x < (SELECT max(y) + 1000 FROM {CLICKHOUSE_DATABASE_1:Identifier}.source);

-- Re-attach the database, so that its tables are not loaded yet.
DETACH DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
ATTACH DATABASE {CLICKHOUSE_DATABASE_1:Identifier};

SELECT rows FROM system.databases WHERE name = {CLICKHOUSE_DATABASE_1:String};
USE {CLICKHOUSE_DATABASE_1:Identifier};
SELECT engine FROM system.tables WHERE database = currentDatabase() AND name = 't';
USE {CLICKHOUSE_DATABASE:Identifier};

RENAME TABLE {CLICKHOUSE_DATABASE_1:Identifier}.t TO {CLICKHOUSE_DATABASE_2:Identifier}.t; -- { serverError TOO_MANY_ROWS }
SELECT count() FROM {CLICKHOUSE_DATABASE_1:Identifier}.t;

DROP DATABASE {CLICKHOUSE_DATABASE_2:Identifier};
DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
