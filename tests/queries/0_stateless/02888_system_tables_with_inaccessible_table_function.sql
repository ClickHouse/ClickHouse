-- Tags: no-fasttest

DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE:Identifier};

CREATE DATABASE {CLICKHOUSE_DATABASE:Identifier};


CREATE TABLE {CLICKHOUSE_DATABASE:Identifier}.tablefunc01 (x int) AS postgresql('127.121.0.1:5432', 'postgres_db', 'postgres_table', 'postgres_user', '124444');
CREATE TABLE {CLICKHOUSE_DATABASE:Identifier}.tablefunc02 (x int) AS mysql('127.123.0.1:3306', 'mysql_db', 'mysql_table', 'mysql_user','123123');
CREATE TABLE {CLICKHOUSE_DATABASE:Identifier}.tablefunc03 (a int) AS sqlite('db_path', 'table_name');
CREATE TABLE {CLICKHOUSE_DATABASE:Identifier}.tablefunc04 (a int) AS mongodb('127.0.0.1:27017','test', 'my_collection', 'test_user', 'password', 'a Int');
CREATE TABLE {CLICKHOUSE_DATABASE:Identifier}.tablefunc05 (a int) AS redis('127.0.0.1:6379', 'key', 'key UInt32');
CREATE TABLE {CLICKHOUSE_DATABASE:Identifier}.tablefunc06 (a int) AS s3('http://some_addr:9000/cloud-storage-01/data.tsv', 'M9O7o0SX5I4udXhWxI12', '9ijqzmVN83fzD9XDkEAAAAAAAA', 'TSV');
-- No format argument, so resolving this table function has to infer the format from the endpoint.
CREATE TABLE {CLICKHOUSE_DATABASE:Identifier}.tablefunc07 (x String) AS url('http://some_addr:9000/nonexistent');


CREATE TABLE {CLICKHOUSE_DATABASE:Identifier}.tablefunc01_without_schema AS postgresql('127.121.0.1:5432', 'postgres_db', 'postgres_table', 'postgres_user', '124444'); -- { serverError POSTGRESQL_CONNECTION_FAILURE }
CREATE TABLE {CLICKHOUSE_DATABASE:Identifier}.tablefunc02_without_schema AS mysql('127.123.0.1:3306', 'mysql_db', 'mysql_table', 'mysql_user','123123'); -- {serverError ALL_CONNECTION_TRIES_FAILED }

SELECT name, engine, engine_full, create_table_query, data_paths, notEmpty([metadata_path]), notEmpty([uuid])
    FROM system.tables
    WHERE name like '%tablefunc%' and database=currentDatabase()
    ORDER BY name;

DETACH TABLE {CLICKHOUSE_DATABASE:Identifier}.tablefunc01;
DETACH TABLE {CLICKHOUSE_DATABASE:Identifier}.tablefunc02;
DETACH TABLE {CLICKHOUSE_DATABASE:Identifier}.tablefunc03;
DETACH TABLE {CLICKHOUSE_DATABASE:Identifier}.tablefunc04;
DETACH TABLE {CLICKHOUSE_DATABASE:Identifier}.tablefunc05;
DETACH TABLE {CLICKHOUSE_DATABASE:Identifier}.tablefunc06;
DETACH TABLE {CLICKHOUSE_DATABASE:Identifier}.tablefunc07;

ATTACH TABLE {CLICKHOUSE_DATABASE:Identifier}.tablefunc01;
ATTACH TABLE {CLICKHOUSE_DATABASE:Identifier}.tablefunc02;
ATTACH TABLE {CLICKHOUSE_DATABASE:Identifier}.tablefunc03;
ATTACH TABLE {CLICKHOUSE_DATABASE:Identifier}.tablefunc04;
ATTACH TABLE {CLICKHOUSE_DATABASE:Identifier}.tablefunc05;
ATTACH TABLE {CLICKHOUSE_DATABASE:Identifier}.tablefunc06;
ATTACH TABLE {CLICKHOUSE_DATABASE:Identifier}.tablefunc07;

SELECT name, engine, engine_full, create_table_query, data_paths, notEmpty([metadata_path]), notEmpty([uuid])
    FROM system.tables
    WHERE name like '%tablefunc%' and database=currentDatabase()
    ORDER BY name;

SELECT count() FROM {CLICKHOUSE_DATABASE:Identifier}.tablefunc01; -- { serverError POSTGRESQL_CONNECTION_FAILURE }
SELECT engine FROM system.tables WHERE name = 'tablefunc01' and database=currentDatabase();

-- Asking a table nobody named for a lock or for its size must not resolve its table function.
SYSTEM STOP MERGES {CLICKHOUSE_DATABASE:Identifier}.tablefunc07;
SYSTEM START MERGES {CLICKHOUSE_DATABASE:Identifier}.tablefunc07;
SELECT lifetime_rows, lifetime_bytes FROM system.tables WHERE database = currentDatabase() AND name = 'tablefunc07';

-- Once the table function is resolved, the nested storage answers again.
CREATE TABLE {CLICKHOUSE_DATABASE:Identifier}.mem (x UInt64) ENGINE = Memory;
INSERT INTO {CLICKHOUSE_DATABASE:Identifier}.mem SELECT number FROM numbers(5);
CREATE TABLE {CLICKHOUSE_DATABASE:Identifier}.tablefunc08 (x UInt64) AS merge(currentDatabase(), '^mem$');
SELECT total_rows, isNotNull(total_bytes) FROM system.tables WHERE database = currentDatabase() AND name = 'tablefunc08';
SELECT count() FROM {CLICKHOUSE_DATABASE:Identifier}.tablefunc08;
SELECT total_rows, isNotNull(total_bytes) FROM system.tables WHERE database = currentDatabase() AND name = 'tablefunc08';

DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE:Identifier};
