-- Tags: no-fasttest

DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE:Identifier};

CREATE DATABASE {CLICKHOUSE_DATABASE:Identifier};


CREATE TABLE {CLICKHOUSE_DATABASE:Identifier}.tablefunc01 (x int) AS postgresql('127.121.0.1:5432', 'postgres_db', 'postgres_table', 'postgres_user', '124444');
CREATE TABLE {CLICKHOUSE_DATABASE:Identifier}.tablefunc02 (x int) AS mysql('127.123.0.1:3306', 'mysql_db', 'mysql_table', 'mysql_user','123123');
CREATE TABLE {CLICKHOUSE_DATABASE:Identifier}.tablefunc03 (a int) As sqlite('db_path', 'table_name');
CREATE TABLE {CLICKHOUSE_DATABASE:Identifier}.tablefunc04 (a int) AS  mongodb('127.0.0.1:27017','test', 'my_collection', 'test_user', 'password', 'a Int');
CREATE TABLE {CLICKHOUSE_DATABASE:Identifier}.tablefunc05 (a int) AS redis('127.0.0.1:6379', 'key', 'key UInt32');

CREATE TABLE {CLICKHOUSE_DATABASE:Identifier}.tablefunc01_without_schema  AS postgresql('127.121.0.1:5432', 'postgres_db', 'postgres_table', 'postgres_user', '124444'); -- { serverError 614 }
CREATE TABLE {CLICKHOUSE_DATABASE:Identifier}.tablefunc02_without_schema  AS mysql('127.123.0.1:3306', 'mysql_db', 'mysql_table', 'mysql_user','123123'); -- {serverError 279 }

SELECT name, engine, engine_full, create_table_query, data_paths, notEmpty([metadata_path]), notEmpty([uuid])
    FROM system.tables
    WHERE name like '%tablefunc%' and database=currentDatabase()
    ORDER BY name;

DETACH TABLE {CLICKHOUSE_DATABASE:Identifier}.tablefunc01; 
DETACH TABLE {CLICKHOUSE_DATABASE:Identifier}.tablefunc02;


ATTACH TABLE {CLICKHOUSE_DATABASE:Identifier}.tablefunc01;
ATTACH TABLE {CLICKHOUSE_DATABASE:Identifier}.tablefunc02;

SELECT name, engine, engine_full, create_table_query, data_paths, notEmpty([metadata_path]), notEmpty([uuid])
    FROM system.tables
    WHERE name like '%tablefunc%' and database=currentDatabase()
    ORDER BY name;

DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE:Identifier};
