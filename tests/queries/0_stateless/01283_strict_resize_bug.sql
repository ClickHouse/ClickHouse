drop table if exists num_10k;
create table num_10k (number UInt64) engine = MergeTree order by tuple();
insert into num_10k select * from numbers(10_000);

select * from (select sum(number) from num_10k union all select sum(number) from num_10k) limit 1 settings max_block_size = 1024;

drop table if exists num_10k;
