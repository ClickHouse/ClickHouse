drop table if exists test.t;
drop table if exists test.z;

create table test.t(a Int64, b Int64) engine = TinyLog;
insert into test.t values(1,1);
insert into test.t values(2,2);
create table test.z(c Int64, d Int64, e Int64) engine = TinyLog;
insert into test.z values(1,1,1);

select * from test.t all left join test.z on (z.c = t.a and z.d = t.b);

drop table if exists test.t;
drop table if exists test.z;

