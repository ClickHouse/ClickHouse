-- Tags: no-parallel

create database replicated_db_no_args engine=Replicated; -- { serverError BAD_ARGUMENTS }
