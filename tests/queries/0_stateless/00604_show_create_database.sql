-- Tags: no-ordinary-database

create database if not exists {CLICKHOUSE_DATABASE:Identifier};
show create database {CLICKHOUSE_DATABASE:Identifier};
drop database {CLICKHOUSE_DATABASE:Identifier};
