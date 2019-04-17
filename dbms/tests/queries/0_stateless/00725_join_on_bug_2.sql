drop table if exists t;
drop table if exists s;

create table t(a Int64, b Int64) engine = TinyLog;
insert into t values(1,1);
insert into t values(2,2);
create table s(a Int64, b Int64) engine = TinyLog;
insert into s values(1,1);

select a, b, s_a, s_b from t all left join (select a,b,a s_a, b s_b from s) using (a,b);
select '-';
select t.*, s.* from t all left join s using (a,b);
select '-';
select a,b,s_a,s_b from t all left join (select a, b, a s_a, b s_b from s) s on (s.a = t.a and s.b = t.b);
select '-';
select * from t all left join (select a s_a, b s_b from s) on (s_a = t.a and s_b = t.b);
select '-';
select a,b,s_a,s_b from t all left join (select a,b, a s_a, b s_b from s) on (s_a = t.a and s_b = t.b);
select '-';
select t.*, s.* from t all left join s on (s.a = t.a and s.b = t.b);

drop table if exists t;
drop table if exists s;
