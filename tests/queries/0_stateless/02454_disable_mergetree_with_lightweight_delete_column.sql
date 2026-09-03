-- Tags: memory-engine
drop table if exists t_row_exists;

create table t_row_exists(a int, _row_exists int) engine=MergeTree order by a; --{serverError ILLEGAL_COLUMN}

create table t_row_exists(a int, b int) engine=MergeTree order by a;
alter table t_row_exists add column _row_exists int; --{serverError ILLEGAL_COLUMN}
alter table t_row_exists rename column b to _row_exists; --{serverError ILLEGAL_COLUMN}
alter table t_row_exists rename column _row_exists to c; --{serverError NOT_FOUND_COLUMN_IN_BLOCK}
alter table t_row_exists drop column _row_exists; --{serverError NOT_FOUND_COLUMN_IN_BLOCK}
alter table t_row_exists drop column unknown_column; --{serverError NOT_FOUND_COLUMN_IN_BLOCK}
drop table t_row_exists;

create table t_row_exists(a int, _row_exists int) engine=Memory;
insert into t_row_exists values(1,1);
select * from t_row_exists;
drop table t_row_exists;

create table t_row_exists(a int, b int) engine=Memory;
alter table t_row_exists add column _row_exists int;
alter table t_row_exists drop column _row_exists;
alter table t_row_exists rename column b to _row_exists;
drop table t_row_exists;
