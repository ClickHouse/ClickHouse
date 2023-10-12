-- Tags: no-fasttest

DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE:Identifier};

CREATE DATABASE {CLICKHOUSE_DATABASE:Identifier};


CREATE TABLE {CLICKHOUSE_DATABASE:Identifier}.tablefunc01 UUID '967e329f-e10a-4a5e-8c27-1ece57daa6a9' (x int) AS postgresql('127.121.0.1:5432', 'postgres_db', 'postgres_table', 'postgres_user', '124444');
CREATE TABLE {CLICKHOUSE_DATABASE:Identifier}.tablefunc02 UUID '997be33d-087b-432b-b90d-c7dc398addf7' (x int) AS mysql('127.123.0.1:3306', 'mysql_db', 'mysql_table', 'mysql_user','123123');


SELECT name, engine, engine_full, create_table_query, data_paths, notEmpty([metadata_path]), uuid
    FROM system.tables
    WHERE name like '%tablefunc%' and database=currentDatabase()
    ORDER BY metadata_modification_time;

DETACH TABLE {CLICKHOUSE_DATABASE:Identifier}.tablefunc01; 
DETACH TABLE {CLICKHOUSE_DATABASE:Identifier}.tablefunc02;


ATTACH TABLE {CLICKHOUSE_DATABASE:Identifier}.tablefunc01 UUID '967e329f-e10a-4a5e-8c27-1ece57daa6a9';
ATTACH TABLE {CLICKHOUSE_DATABASE:Identifier}.tablefunc02 UUID '997be33d-087b-432b-b90d-c7dc398addf7';

SELECT name, engine, engine_full, create_table_query, data_paths, notEmpty([metadata_path]), uuid
    FROM system.tables
    WHERE name like '%tablefunc%' and database=currentDatabase()
    ORDER BY metadata_modification_time;

DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE:Identifier};
