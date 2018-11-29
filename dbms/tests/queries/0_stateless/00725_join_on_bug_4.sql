drop table if exists test.t;
drop table if exists test.s;

create table test.t(a Int64, b Int64, c String) engine = TinyLog;
insert into test.t values(1,1,'a'),(2,2,'b');
create table test.s(a Int64, b Int64, c String) engine = TinyLog;
insert into test.s values(1,1,'a');


select t.* from test.t all left join test.s on (s.a = t.a and s.b = t.b) where s.a = 0 and s.b = 0;

drop table if exists test.t;
drop table if exists test.s;
