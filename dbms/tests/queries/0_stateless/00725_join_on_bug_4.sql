drop table if exists t;
drop table if exists s;

create table t(a Int64, b Int64, c String) engine = TinyLog;
insert into t values(1,1,'a'),(2,2,'b');
create table s(a Int64, b Int64, c String) engine = TinyLog;
insert into s values(1,1,'a');


select t.* from t all left join s on (s.a = t.a and s.b = t.b) where s.a = 0 and s.b = 0;

drop table if exists t;
drop table if exists s;
