drop table if exists t;

create table t (id UInt32, a Int) engine = MergeTree order by id settings min_bytes_for_wide_part=0;

insert into t values (1, 0) (2, 1) (3, 0) (4, 0) (5, 0);
alter table t add column s String default 'foo';
select s from t prewhere a = 1;

drop table t;

create table t (id UInt32, a Int) engine = MergeTree order by id settings min_bytes_for_wide_part=0;

insert into t values (1, 1) (2, 1) (3, 0) (4, 0) (5, 0);
alter table t add column s String default 'foo';
select s from t prewhere a = 1;

drop table t;
