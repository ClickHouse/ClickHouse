create or replace table t engine = Memory as with cte as (select * from numbers(10)) select * from cte;
drop table t;
