drop table if exists data_01593;
create table data_01593 (key Int) engine=MergeTree() order by key partition by key;

insert into data_01593 select * from numbers_mt(10);
insert into data_01593 select * from numbers_mt(10) settings max_partitions_per_insert_block=1; -- { serverError TOO_MANY_PARTS }
-- throw_on_max_partitions_per_insert_block=false means we'll just log that the limit was reached rather than throw
insert into data_01593 select * from numbers_mt(10) settings max_partitions_per_insert_block=1, throw_on_max_partitions_per_insert_block=false;
-- settings for INSERT is prefered
insert into data_01593 settings max_partitions_per_insert_block=100 select * from numbers_mt(10) settings max_partitions_per_insert_block=1;

drop table data_01593;
