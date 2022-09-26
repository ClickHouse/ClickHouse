drop table if exists t_row_exists;

create table t_row_exists(a int, _row_exists int) engine=MergeTree order by a; --{serverError 44}

create table t_row_exists(a int, b int) engine=MergeTree order by a;
alter table t_row_exists add column _row_exists int; --{serverError 44}
alter table t_row_exists rename column b to _row_exists; --{serverError 44}
drop table t_row_exists;

create table t_row_exists(a int, _row_exists int) engine=Memory;
insert into t_row_exists values(1,1);
select * from t_row_exists;
drop table t_row_exists;

create table t_row_exists(a int, b int) engine=Memory;
alter table t_row_exists add column _row_exists int; --{serverError 48}
alter table t_row_exists rename column b to _row_exists; --{serverError 48}
drop table t_row_exists;
