drop table if exists t;
create table t engine = Memory as with cte as (select * from numbers(10)) select * from cte;
drop table t;

drop table if exists view1;
create view view1 as with t as (select number n from numbers(3)) select n from t;
drop table view1;
