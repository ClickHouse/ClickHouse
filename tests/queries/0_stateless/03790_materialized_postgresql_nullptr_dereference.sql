-- Tags: no-fasttest
-- depends on libpq

SET allow_experimental_database_materialized_postgresql = 1;
CREATE DATABASE d03790_materialized_postgresql_nullptr_dereference ENGINE = MaterializedPostgreSQL; -- { serverError BAD_ARGUMENTS }
CREATE DATABASE d03790_materialized_postgresql_nullptr_dereference ENGINE = PostgreSQL; -- { serverError BAD_ARGUMENTS }
