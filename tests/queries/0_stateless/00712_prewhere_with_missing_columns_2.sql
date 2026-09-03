drop table if exists t_00712_1;
create table t_00712_1 (a Int32, b Int32) engine = MergeTree partition by (a,b) order by (a);

insert into t_00712_1 values (1, 1);
alter table t_00712_1 add column c Int32;

select b from t_00712_1 prewhere a < 1000;
select c from t_00712_1 where a < 1000;
select c from t_00712_1 prewhere a < 1000;

drop table t_00712_1;

