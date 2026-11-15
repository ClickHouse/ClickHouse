-- Tags: no-fasttest
-- depends on libpq

SET allow_experimental_database_materialized_postgresql = 1;
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier} ENGINE = MaterializedPostgreSQL; -- { serverError BAD_ARGUMENTS }
CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier} ENGINE = PostgreSQL; -- { serverError BAD_ARGUMENTS }
