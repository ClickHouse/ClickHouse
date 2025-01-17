drop table if exists t;
drop table if exists s;

create table t(a Int64, b Int64, c String) engine = Memory;
create table s(a Int64, b Int64, c String) engine = Memory;

insert into t values(1,1,'a');
insert into s values(2,2,'a');

select t.*, s.a, s.b, s.c from t left join s on (s.a = t.a and s.b = t.b);
select t.*, s.a, s.b, s.c from t right join s on (s.a = t.a and s.b = t.b);
select t.*, s.a, s.b, s.c from t left join s on (s.a = t.a and s.b = t.b) SETTINGS join_use_nulls = 1;
select t.*, s.a, s.b, s.c from t right join s on (s.a = t.a and s.b = t.b) SETTINGS join_use_nulls = 1;

drop table t;
drop table s;

create table t(a Int64, b Int64, c Nullable(String)) engine = Memory;
create table s(a Int64, b Int64, c Nullable(String)) engine = Memory;

insert into t values(1,1,'a');
insert into s values(2,2,'a');

select * from t left join s on (s.a = t.a and s.b = t.b);
select * from t right join s on (s.a = t.a and s.b = t.b);
select t.*, s.* from t left join s on (s.a = t.a and s.b = t.b) SETTINGS join_use_nulls = 1;
select t.*, s.* from t right join s on (s.a = t.a and s.b = t.b) SETTINGS join_use_nulls = 1;

drop table t;
drop table s;

create table t(a Int64, b Nullable(Int64), c String) engine = Memory;
create table s(a Int64, b Nullable(Int64), c String) engine = Memory;

insert into t values(1,1,'a');
insert into s values(2,2,'a');

select t.*, s.* from t left join s on (s.a = t.a and s.b = t.b);
select t.*, s.* from t right join s on (s.a = t.a and s.b = t.b);
select * from t left join s on (s.a = t.a and s.b = t.b) SETTINGS join_use_nulls = 1;
select * from t right join s on (s.a = t.a and s.b = t.b) SETTINGS join_use_nulls = 1;

drop table t;
drop table s;

create table t(a Int64, b Nullable(Int64), c Nullable(String)) engine = Memory;
create table s(a Int64, b Nullable(Int64), c Nullable(String)) engine = Memory;

insert into t values(1,1,'a');
insert into s values(2,2,'a');

select t.*, s.a, s.b, s.c from t left join s on (s.a = t.a and s.b = t.b);
select t.*, s.a, s.b, s.c from t right join s on (s.a = t.a and s.b = t.b);
select * from t left join s on (s.a = t.a and s.b = t.b) SETTINGS join_use_nulls = 1;
select * from t right join s on (s.a = t.a and s.b = t.b) SETTINGS join_use_nulls = 1;

drop table t;
drop table s;

create table t(a Nullable(Int64), b Nullable(Int64), c Nullable(String)) engine = Memory;
create table s(a Nullable(Int64), b Nullable(Int64), c Nullable(String)) engine = Memory;

insert into t values(1,1,'a');
insert into s values(2,2,'a');

select * from t left join s on (s.a = t.a and s.b = t.b);
select * from t right join s on (s.a = t.a and s.b = t.b);
select t.*, s.a, s.b, s.c from t left join s on (s.a = t.a and s.b = t.b) SETTINGS join_use_nulls = 1;
select t.*, s.a, s.b, s.c from t right join s on (s.a = t.a and s.b = t.b) SETTINGS join_use_nulls = 1;

drop table t;
drop table s;
