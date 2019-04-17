drop table if exists t;
drop table if exists z;

create table t(a Int64, b Int64) engine = TinyLog;
insert into t values(1,1);
insert into t values(2,2);
create table z(c Int64, d Int64, e Int64) engine = TinyLog;
insert into z values(1,1,1);

select * from t all left join z on (z.c = t.a and z.d = t.b);

drop table if exists t;
drop table if exists z;

