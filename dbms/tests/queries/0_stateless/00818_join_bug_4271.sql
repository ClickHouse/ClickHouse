use test;

drop table if exists t;
drop table if exists s;

create table t(a Nullable(Int64), b Nullable(Int64), c Nullable(String)) engine = Memory;
create table s(a Nullable(Int64), b Nullable(Int64), c Nullable(String)) engine = Memory;

insert into t values(1,1,'a'), (2,2,'b');
insert into s values(1,1,'a');

select * from t left join s on t.a = s.a;
select * from t left join s on t.a = s.a and t.a = s.b;
select * from t left join s on t.a = s.a where s.a = 1;
select * from t left join s on t.a = s.a and t.a = s.a;
select * from t left join s on t.a = s.a and t.b = s.a;

drop table t;
drop table s;
