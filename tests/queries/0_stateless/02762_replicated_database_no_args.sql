-- Tags: no-parallel

create database {CLICKHOUSE_DATABASE_1:Identifier} engine=Replicated; -- { serverError BAD_ARGUMENTS }
