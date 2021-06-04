drop table if exists data_01593;
create table data_01593 (key Int) engine=MergeTree() order by key partition by key;

insert into data_01593 select * from numbers_mt(10);
-- TOO_MANY_PARTS error
insert into data_01593 select * from numbers_mt(10) settings max_partitions_per_insert_block=1; -- { serverError 252 }
-- settings for INSERT is prefered
insert into data_01593 select * from numbers_mt(10) settings max_partitions_per_insert_block=1 settings max_partitions_per_insert_block=100;

drop table data_01593;
