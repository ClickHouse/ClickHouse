-- Tags: no-parallel

SET create_if_not_exists=0;  -- Default

DROP TABLE IF EXISTS example_table;
CREATE TABLE example_table (id UInt32) ENGINE=MergeTree() ORDER BY id;
CREATE TABLE example_table (id UInt32) ENGINE=MergeTree() ORDER BY id; -- { serverError TABLE_ALREADY_EXISTS }

DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier};
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier}; -- { serverError DATABASE_ALREADY_EXISTS }

SET create_if_not_exists=1;

DROP TABLE IF EXISTS example_table;
CREATE TABLE example_table (id UInt32) ENGINE=MergeTree() ORDER BY id;
CREATE TABLE example_table (id UInt32) ENGINE=MergeTree() ORDER BY id;

DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier};
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier};

DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier};
DROP TABLE IF EXISTS example_table;