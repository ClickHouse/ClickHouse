set ignore_materialized_views_with_dropped_target_table = 1;
set send_logs_level='error';
drop table if exists from_table;
drop table if exists to_table;
drop table if exists mv;

create table from_table (x UInt32) engine=MergeTree order by x;
create table to_table (x UInt32) engine=MergeTree order by x;
create materialized view mv to to_table as select * from from_table;

insert into from_table select 42;
select * from from_table;
select * from to_table;

drop table to_table;

insert into from_table select 42;
select * from from_table;

drop table from_table;
drop view mv;
