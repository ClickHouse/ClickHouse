drop table if exists t_00725_3;
drop table if exists z_00725_3;

create table t_00725_3(a Int64, b Int64) engine = TinyLog;
insert into t_00725_3 values(1,1);
insert into t_00725_3 values(2,2);
create table z_00725_3(c Int64, d Int64, e Int64) engine = TinyLog;
insert into z_00725_3 values(1,1,1);

select * from t_00725_3 all left join z_00725_3 on (z_00725_3.c = t_00725_3.a and z_00725_3.d = t_00725_3.b);

drop table if exists t_00725_3;
drop table if exists z_00725_3;

