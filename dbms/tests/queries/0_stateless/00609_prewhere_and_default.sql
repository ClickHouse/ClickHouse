drop table if exists `table`;
create table `table` (key UInt64, val UInt64) engine = MergeTree order by key settings index_granularity=8192;
insert into `table` select number, number / 8192 from system.numbers limit 100000; 
alter table `table` add column def UInt64 default val + 1;
select * from `table` prewhere val > 2 format Null;

drop table if exists `table`;
create table `table` (key UInt64, val UInt64) engine = MergeTree order by key settings index_granularity=8192;
insert into `table` select number, number / 8192 from system.numbers limit 100000; 
alter table `table` add column def UInt64;
select * from `table` prewhere val > 2 format Null;

drop table if exists `table`;
