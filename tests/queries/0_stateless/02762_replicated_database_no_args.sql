-- Tags: no-parallel

set allow_experimental_database_replicated=1;
create database replicated_db_no_args engine=Replicated; -- { serverError BAD_ARGUMENTS }
