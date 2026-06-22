-- Tags: no-parallel

DROP DATABASE IF EXISTS test2960_valid_database_engine;

-- create database with valid engine. Should succeed.
CREATE DATABASE test2960_valid_database_engine ENGINE = Atomic;

-- create database with valid engine but arguments are not allowed. Should fail.
CREATE DATABASE test2960_database_engine_args_not_allowed ENGINE = Atomic('foo', 'bar'); -- { serverError BAD_ARGUMENTS }

-- create database with an invalid engine. Should fail.
CREATE DATABASE test2960_invalid_database_engine ENGINE = Foo; -- { serverError UNKNOWN_DATABASE_ENGINE }

DROP DATABASE IF EXISTS test2960_valid_database_engine;
