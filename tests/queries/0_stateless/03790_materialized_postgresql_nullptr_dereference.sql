-- Tags: no-fasttest
-- depends on libpq

SET allow_experimental_database_materialized_postgresql = 1;
CREATE DATABASE test ENGINE = MaterializedPostgreSQL; -- { serverError BAD_ARGUMENTS }
CREATE DATABASE test ENGINE = PostgreSQL; -- { serverError BAD_ARGUMENTS }
