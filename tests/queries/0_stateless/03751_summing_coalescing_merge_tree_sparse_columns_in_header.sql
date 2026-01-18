set allow_suspicious_primary_key = 1;
drop table if exists src;
create table src (x UInt64) engine=MergeTree order by tuple();
insert into src select 0 from numbers(1000000);
drop table if exists dst;
create table dst (x UInt64) engine=CoalescingMergeTree order by tuple();
insert into dst select * from src;
drop table dst;
create table dst (x UInt64) engine=SummingMergeTree order by tuple();
insert into dst select * from src;
drop table dst;
drop table src;

