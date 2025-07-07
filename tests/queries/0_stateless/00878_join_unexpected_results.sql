drop table if exists t;
drop table if exists s;

create table t(a Int64, b Int64) engine = Memory;
create table s(a Int64, b Int64) engine = Memory;

insert into t values (1,1), (2,2);
insert into s values (1,1);

select 'join_use_nulls = 1';
set join_use_nulls = 1;
select * from t left outer join s using (a,b) order by t.a;
select '-';
select * from t join s using (a,b);
select '-';
select * from t join s on (t.a=s.a and t.b=s.b);
select '-';
select t.* from t left join s on (t.a=s.a and t.b=s.b) order by t.a;
select '-';
select t.*, s.* from t left join s on (t.a=s.a and t.b=s.b) order by t.a;
select '-';
select t.*, s.* from t left join s on (s.a=t.a and t.b=s.b) order by t.a;
select '-';
select t.*, s.* from t right join s on (t.a=s.a and t.b=s.b);
select '-';
select * from t left outer join s using (a,b) where s.a is null;
select '-';
select * from t left outer join s on (t.a=s.a and t.b=s.b) where s.a is null;
select '-';
select s.* from t left outer join s on (t.a=s.a and t.b=s.b) where s.a is null;
select '-';
select t.*, s.* from t left join s on (s.a=t.a and t.b=s.b and t.a=toInt64(2)) order by t.a;
select '-';
select t.*, s.* from t left join s on (s.a=t.a) order by t.a;
select '-';
select t.*, s.* from t left join s on (t.b=toInt64(1) and s.a=t.a) where s.b=1;
select '-';
select t.*, s.* from t left join s on (t.b=toInt64(2) and s.a=t.a) where t.b=2;

select 'join_use_nulls = 0';
set join_use_nulls = 0;
select * from t left outer join s using (a,b) order by t.a;
select '-';
select * from t join s using (a,b);
select '-';
select * from t join s on (t.a=s.a and t.b=s.b);
select '-';
select t.* from t left join s on (t.a=s.a and t.b=s.b) order by t.a;
select '-';
select t.*, s.* from t left join s on (t.a=s.a and t.b=s.b) order by t.a;
select '-';
select t.*, s.* from t left join s on (s.a=t.a and t.b=s.b) order by t.a;
select '-';
select t.*, s.* from t right join s on (t.a=s.a and t.b=s.b);
select '-';
-- select * from t left outer join s using (a,b) where s.a is null; -- TODO
select '-';
-- select * from t left outer join s on (t.a=s.a and t.b=s.b) where s.a is null; -- TODO
select '-';
-- select s.* from t left outer join s on (t.a=s.a and t.b=s.b) where s.a is null; -- TODO
select '-';
select t.*, s.* from t left join s on (s.a=t.a and t.b=s.b and t.a=toInt64(2)) order by t.a;
select '-';
select t.*, s.* from t left join s on (s.a=t.a) order by t.a;
select '-';
select t.*, s.* from t left join s on (t.b=toInt64(1) and s.a=t.a) where s.b=1;
select '-';
select t.*, s.* from t left join s on (t.b=toInt64(2) and s.a=t.a) where t.b=2;

drop table t;
drop table s;
