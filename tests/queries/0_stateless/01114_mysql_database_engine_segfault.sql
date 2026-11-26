-- Tags: no-fasttest

DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier};
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier} ENGINE = MySQL('127.0.0.1:3456', conv_main, 'metrika', 'password'); -- { serverError CANNOT_CREATE_DATABASE }
