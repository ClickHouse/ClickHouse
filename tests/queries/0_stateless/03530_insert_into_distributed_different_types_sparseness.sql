-- Ensure that sparse columns does not leads to any errors/warnings while pushing via Distributed

drop table if exists sparse;
drop table if exists intermediate;
drop table if exists non_sparse;
drop table if exists non_sparse_remote;
drop table if exists mv_non_sparse;
drop table if exists log;
drop table if exists log_remote;
drop table if exists mv_log;

create table sparse (key String) engine=MergeTree order by () settings ratio_of_defaults_for_sparse_serialization=0.01;
insert into sparse select ''::String from numbers(100);
select dumpColumnStructure(*) from sparse limit 1;

-- we need a table that supports sparse columns as intermediate, hence MergeTree
create table intermediate (key String) engine=MergeTree order by ();

create table non_sparse (key String) engine=MergeTree order by () settings ratio_of_defaults_for_sparse_serialization=1;
create table non_sparse_remote as remote('127.1', currentDatabase(), non_sparse);
create materialized view mv_non_sparse to non_sparse_remote as select * from intermediate;

-- now ensure that insert into Log will not break anything
create table log (key String) engine=Log;
create table log_remote as remote('127.1', currentDatabase(), log);
create materialized view mv_log to log as select * from intermediate;

insert into intermediate select * from sparse;
select count() from non_sparse;
select count() from mv_log;
