drop table if exists num_10m;
create table num_10m (number UInt64) engine = MergeTree order by tuple();
insert into num_10m select * from numbers(10000000);

select * from (select sum(number) from num_10m union all select sum(number) from num_10m) limit 1 settings max_block_size = 1024;

drop table if exists num_1m;
