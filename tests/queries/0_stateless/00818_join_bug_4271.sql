drop table if exists t_00818;
drop table if exists s_00818;

create table t_00818(a Nullable(Int64), b Nullable(Int64), c Nullable(String)) engine = Memory;
create table s_00818(a Nullable(Int64), b Nullable(Int64), c Nullable(String)) engine = Memory;

insert into t_00818 values(1,1,'a'), (2,2,'b');
insert into s_00818 values(1,1,'a');

select * from t_00818 left join s_00818 on t_00818.a = s_00818.a;
select * from t_00818 left join s_00818 on t_00818.a = s_00818.a and t_00818.a = s_00818.b;
select * from t_00818 left join s_00818 on t_00818.a = s_00818.a where s_00818.a = 1;
select * from t_00818 left join s_00818 on t_00818.a = s_00818.a and t_00818.a = s_00818.a;
select * from t_00818 left join s_00818 on t_00818.a = s_00818.a and t_00818.b = s_00818.a;

drop table t_00818;
drop table s_00818;
