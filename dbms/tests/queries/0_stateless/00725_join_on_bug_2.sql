drop table if exists test.t;
drop table if exists test.s;

create table test.t(a Int64, b Int64) engine = TinyLog;
insert into test.t values(1,1);
insert into test.t values(2,2);
create table test.s(a Int64, b Int64) engine = TinyLog;
insert into test.s values(1,1);

select a, b, s_a, s_b from test.t all left join (select a,b,a s_a, b s_b from test.s) using (a,b);
select '-';
select * from test.t all left join test.s using (a,b);
select '-';
select a,b,s_a,s_b from test.t all left join (select a, b, a s_a, b s_b from test.s) s on (s.a = t.a and s.b = t.b);
select '-';
select * from test.t all left join (select a s_a, b s_b from test.s) on (s_a = t.a and s_b = t.b);
select '-';
select a,b,s_a,s_b from test.t all left join (select a,b, a s_a, b s_b from test.s) on (s_a = t.a and s_b = t.b);
select '-';
select t.*, s.* from test.t all left join test.s on (s.a = t.a and s.b = t.b);

drop table if exists test.t;
drop table if exists test.s;
