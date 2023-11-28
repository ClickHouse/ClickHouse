drop table if exists data_mt;
drop table if exists view_mt;
drop table if exists data_mem;
drop table if exists view_mem;

create table data_mt (key Int) engine=MergeTree() order by (key, sipHash64(key)) sample by sipHash64(key);
insert into data_mt select * from numbers(10);
create view view_mt as select * from data_mt;

create table data_mem (key Int) engine=Memory();
insert into data_mem select * from numbers(10);
create view view_mem as select * from data_mem;

-- { echo }
select * from data_mt sample 0.1 order by key;
select * from view_mt sample 0.1 order by key;
select * from data_mem sample 0.1 order by key; -- { serverError SAMPLING_NOT_SUPPORTED }
select * from view_mem sample 0.1 order by key; -- { serverError SAMPLING_NOT_SUPPORTED }
